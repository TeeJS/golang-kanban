package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

// ---------------------------------------------------------------------------
// Domain types
// ---------------------------------------------------------------------------

type Card struct {
	ID          int
	Title       string
	Description string
	Subtasks    string
	Status      string
	Category    string
	CardOrder   int
	CreatedAt   time.Time
	UpdatedAt   time.Time
	DueOn       sql.NullTime
}

type Subtask struct {
	Completed bool
	Text      string
}

type Category struct {
	ID       int
	Name     string
	Slug     string
	RowOrder int
	Locked   bool
}

type Status struct {
	ID       int
	Name     string
	Slug     string
	ColOrder int
	Locked   bool
}

type StatusColumn struct {
	Status Status
	Cards  []Card
}

type CategoryRow struct {
	Category Category
	Columns  []StatusColumn
	ColCount int
}

type BoardTemplateData struct {
	Rows                      []CategoryRow
	Categories                []Category
	Statuses                  []Status
	HelpdeskRefreshInterval   int
	UnassignedRefreshInterval int
	APIKey                    string
}

type FreshserviceTicket struct {
	ID      int    `json:"id"`
	Subject string `json:"subject"`
	Status  int    `json:"status"`
	GroupID int    `json:"group_id"`
	DueBy   string `json:"due_by"`
}

type FreshserviceResponse struct {
	Tickets []FreshserviceTicket `json:"tickets"`
}

// ---------------------------------------------------------------------------
// Auth types
// ---------------------------------------------------------------------------

type contextKey string

const contextKeyUser contextKey = "user"

type User struct {
	ID           int
	Username     string
	PasswordHash string
}

type SessionData struct {
	UserID   int    `json:"u"`
	Username string `json:"n"`
	Expires  int64  `json:"e"`
}

type TicketTask struct {
	ID      int    `json:"id"`
	Status  int    `json:"status"`
	DueDate string `json:"due_date"`
	GroupID int    `json:"group_id"`
	Deleted bool   `json:"deleted"`
}

type TicketTasksResponse struct {
	Tasks []TicketTask `json:"tasks"`
}

type UnassignedCard struct {
	TicketID int
	Subject  string
	GroupID  int
	DueBy    string
	IsTask   bool
}

type HelpdeskColumn struct {
	Name    string
	Tickets []FreshserviceTicket
}

type OrderUpdatePayload struct {
	Category string `json:"category"`
	Status   string `json:"status"`
	Order    []int  `json:"order"`
}

type SlugOrderPayload struct {
	Order []string `json:"order"`
}

// ---------------------------------------------------------------------------
// Globals
// ---------------------------------------------------------------------------

var db *sql.DB
var tmpl *template.Template
var fsAPIKey string
var fsDomain string
var sessionSecret []byte

// SSE broadcaster
var sseClients = make(map[chan string]struct{})
var sseMu sync.Mutex

func broadcastBoardUpdate() {
	sseMu.Lock()
	defer sseMu.Unlock()
	for ch := range sseClients {
		select {
		case ch <- "board-updated":
		default:
		}
	}
}

// ---------------------------------------------------------------------------
// Startup
// ---------------------------------------------------------------------------

func main() {
	dbUser := getEnv("DB_USER", "user")
	dbPass := getEnv("DB_PASS", "password")
	dbHost := getEnv("DB_HOST", "postgres")
	dbPort := getEnv("DB_PORT", "5432")
	dbName := getEnv("DB_NAME", "kanban")

	connStr := "postgres://" + dbUser + ":" + dbPass + "@" + dbHost + ":" + dbPort + "/" + dbName + "?sslmode=disable"

	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(1)

	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("Error closing database: %v", err)
		}
	}()

	if err := runMigrations(); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	// Session secret
	secret := getEnv("SESSION_SECRET", "")
	if secret == "" {
		log.Println("WARNING: SESSION_SECRET not set — sessions will not survive restarts. Set SESSION_SECRET in your environment.")
		sessionSecret = make([]byte, 32)
		if _, err := rand.Read(sessionSecret); err != nil {
			log.Fatalf("Failed to generate session secret: %v", err)
		}
	} else {
		sessionSecret = []byte(secret)
	}

	seedAdminUser()

	fsAPIKey = getEnv("FRESHSERVICE_APIKEY", "")
	fsDomain = getEnv("FRESHSERVICE_DOMAIN", "")

	funcMap := template.FuncMap{
		"truncate": func(s string, n int) string {
			r := []rune(s)
			if len(r) <= n {
				return s
			}
			return string(r[:n]) + "…"
		},
		"split": func(s, sep string) []string {
			s = strings.TrimSpace(s)
			if s == "" {
				return nil
			}
			return strings.Split(s, sep)
		},
		"trim":                strings.TrimSpace,
		"parseSubtasks":       parseSubtasks,
		"hasSubtasks":         hasSubtasks,
		"allSubtasksComplete": allSubtasksComplete,
		"isOverdue": func(d sql.NullTime) bool {
			if !d.Valid {
				return false
			}
			today := time.Now().Truncate(24 * time.Hour)
			return d.Time.Before(today)
		},
		"isTicketOverdue": func(dueBy string) bool {
			if dueBy == "" {
				return false
			}
			t, err := time.Parse(time.RFC3339, dueBy)
			if err != nil {
				return false
			}
			today := time.Now().Truncate(24 * time.Hour)
			return !t.After(today)
		},
		"colGridClass": func(n int) string {
			switch {
			case n <= 2:
				return "grid-cols-1 md:grid-cols-2"
			case n == 3:
				return "grid-cols-1 md:grid-cols-3"
			case n == 4:
				return "grid-cols-1 md:grid-cols-2 xl:grid-cols-4"
			default:
				return "grid-cols-1 md:grid-cols-2 xl:grid-cols-5"
			}
		},
	}

	tmpl = template.Must(template.New("").Funcs(funcMap).ParseGlob("templates/*.html"))

	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})

	http.HandleFunc("/login", loginHandler)
	http.HandleFunc("/logout", logoutHandler)

	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/board", boardHandler)
	http.HandleFunc("/card", createCardHandler)
	http.HandleFunc("/card/", cardRouter)
	http.HandleFunc("/card/order", updateOrderHandler)
	http.HandleFunc("/category", createCategoryHandler)
	http.HandleFunc("/category/", categoryRouter)
	http.HandleFunc("/status", createStatusHandler)
	http.HandleFunc("/status/", statusRouter)
	http.HandleFunc("/api/cards", apiCardsHandler)
	http.HandleFunc("/api/categories", apiCategoriesHandler)
	http.HandleFunc("/api/statuses", apiStatusesHandler)
	http.HandleFunc("/api/settings", settingsHandler)
	http.HandleFunc("/api/rotate-key", rotateApiKeyHandler)
	http.HandleFunc("/helpdesk/fragment", helpdeskFragmentHandler)
	http.HandleFunc("/unassigned/fragment", unassignedFragmentHandler)
	http.HandleFunc("/events", sseHandler)

	serverPort := getEnv("SERVER_PORT", "17808")
	log.Println("Server started on :" + serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, authMiddleware(http.DefaultServeMux)))
}

func runMigrations() error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS categories (
			id        SERIAL PRIMARY KEY,
			name      TEXT NOT NULL,
			slug      TEXT NOT NULL UNIQUE,
			row_order INTEGER NOT NULL DEFAULT 0,
			locked    BOOLEAN NOT NULL DEFAULT false
		)`,
		`CREATE TABLE IF NOT EXISTS statuses (
			id        SERIAL PRIMARY KEY,
			name      TEXT NOT NULL,
			slug      TEXT NOT NULL UNIQUE,
			col_order INTEGER NOT NULL DEFAULT 0,
			locked    BOOLEAN NOT NULL DEFAULT false
		)`,
		`ALTER TABLE cards ADD COLUMN IF NOT EXISTS category VARCHAR(50) NOT NULL DEFAULT 'work'`,
		`ALTER TABLE cards ALTER COLUMN status TYPE VARCHAR(50)`,
		`INSERT INTO categories (name, slug, row_order, locked) VALUES
			('Work',     'work',     1, false),
			('Personal', 'personal', 2, false),
			('Other',    'other',    3, false)
		ON CONFLICT (slug) DO NOTHING`,
		`INSERT INTO statuses (name, slug, col_order, locked) VALUES
			('Tomorrow',       'tomorrow',      1, false),
			('To Do',          'todo',          2, true),
			('In Progress',    'inprogress',    3, false),
			('Needs Feedback', 'needsfeedback', 4, false),
			('Done',           'done',          5, true)
		ON CONFLICT (slug) DO NOTHING`,
		// Migrate any legacy stardom cards to other
		`UPDATE cards SET category = 'other' WHERE category = 'stardom'`,
		`ALTER TABLE cards ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`ALTER TABLE cards ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()`,
		`ALTER TABLE cards ADD COLUMN IF NOT EXISTS due_on DATE`,
		`CREATE TABLE IF NOT EXISTS settings (
			key   TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`INSERT INTO settings (key, value) VALUES ('helpdesk_refresh_interval', '300') ON CONFLICT (key) DO NOTHING`,
		`INSERT INTO settings (key, value) VALUES ('unassigned_refresh_interval', '300') ON CONFLICT (key) DO NOTHING`,
		`CREATE TABLE IF NOT EXISTS users (
			id            SERIAL PRIMARY KEY,
			username      VARCHAR(50) UNIQUE NOT NULL,
			password_hash VARCHAR(255) NOT NULL,
			created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
			updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
		)`,
		`ALTER TABLE users ADD COLUMN IF NOT EXISTS api_key VARCHAR(64)`,
	}

	for _, m := range migrations {
		if _, err := db.Exec(m); err != nil {
			return err
		}
	}
	return nil
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func generateApiKey() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("Failed to generate API key: %v", err)
	}
	return base64.RawURLEncoding.EncodeToString(b)
}

func getUserApiKey(userID int) string {
	var key sql.NullString
	if err := db.QueryRow(`SELECT api_key FROM users WHERE id=$1`, userID).Scan(&key); err != nil {
		return ""
	}
	if key.Valid && key.String != "" {
		return key.String
	}
	newKey := generateApiKey()
	if _, err := db.Exec(`UPDATE users SET api_key=$1 WHERE id=$2`, newKey, userID); err != nil {
		return ""
	}
	return newKey
}

func getSettingInt(key string, defaultVal int) int {
	var val string
	err := db.QueryRow(`SELECT value FROM settings WHERE key=$1`, key).Scan(&val)
	if err != nil {
		return defaultVal
	}
	n, err := strconv.Atoi(val)
	if err != nil {
		return defaultVal
	}
	return n
}

func setSettingInt(key string, val int) error {
	_, err := db.Exec(`UPDATE settings SET value=$1 WHERE key=$2`, strconv.Itoa(val), key)
	return err
}

// ---------------------------------------------------------------------------
// DB helpers
// ---------------------------------------------------------------------------

func getCategories() ([]Category, error) {
	rows, err := db.Query(`SELECT id, name, slug, row_order, locked FROM categories ORDER BY row_order, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cats []Category
	for rows.Next() {
		var c Category
		if err := rows.Scan(&c.ID, &c.Name, &c.Slug, &c.RowOrder, &c.Locked); err != nil {
			return nil, err
		}
		cats = append(cats, c)
	}
	return cats, nil
}

func getStatuses() ([]Status, error) {
	rows, err := db.Query(`SELECT id, name, slug, col_order, locked FROM statuses ORDER BY col_order, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statuses []Status
	for rows.Next() {
		var s Status
		if err := rows.Scan(&s.ID, &s.Name, &s.Slug, &s.ColOrder, &s.Locked); err != nil {
			return nil, err
		}
		statuses = append(statuses, s)
	}
	return statuses, nil
}

func isValidCategory(slug string) bool {
	var exists bool
	err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM categories WHERE slug=$1)`, slug).Scan(&exists)
	return err == nil && exists
}

func isValidStatus(slug string) bool {
	var exists bool
	err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM statuses WHERE slug=$1)`, slug).Scan(&exists)
	return err == nil && exists
}

func getCardByID(id int) (*Card, error) {
	var card Card
	err := db.QueryRow(
		`SELECT id, title, description, subtasks, status, category, card_order, created_at, updated_at, due_on FROM cards WHERE id=$1`,
		id,
	).Scan(&card.ID, &card.Title, &card.Description, &card.Subtasks, &card.Status, &card.Category, &card.CardOrder, &card.CreatedAt, &card.UpdatedAt, &card.DueOn)
	if err != nil {
		return nil, err
	}
	return &card, nil
}

func buildBoardData() (*BoardTemplateData, error) {
	cats, err := getCategories()
	if err != nil {
		return nil, err
	}

	statuses, err := getStatuses()
	if err != nil {
		return nil, err
	}

	// Build index maps for fast lookup
	catIndex := make(map[string]int, len(cats))
	for i, c := range cats {
		catIndex[c.Slug] = i
	}
	statusIndex := make(map[string]int, len(statuses))
	for i, s := range statuses {
		statusIndex[s.Slug] = i
	}

	// Build rows skeleton
	rows := make([]CategoryRow, len(cats))
	for i, cat := range cats {
		cols := make([]StatusColumn, len(statuses))
		for j, st := range statuses {
			cols[j] = StatusColumn{Status: st}
		}
		rows[i] = CategoryRow{Category: cat, Columns: cols, ColCount: len(cols)}
	}

	// Query cards
	cardRows, err := db.Query(`
		SELECT id, title, description, subtasks, status, category, card_order, created_at, updated_at, due_on
		FROM cards
		ORDER BY card_order, id
	`)
	if err != nil {
		return nil, err
	}
	defer cardRows.Close()

	for cardRows.Next() {
		var c Card
		if err := cardRows.Scan(&c.ID, &c.Title, &c.Description, &c.Subtasks, &c.Status, &c.Category, &c.CardOrder, &c.CreatedAt, &c.UpdatedAt, &c.DueOn); err != nil {
			continue
		}
		catI, okCat := catIndex[c.Category]
		stI, okSt := statusIndex[c.Status]
		if !okCat || !okSt {
			continue
		}
		rows[catI].Columns[stI].Cards = append(rows[catI].Columns[stI].Cards, c)
	}

	return &BoardTemplateData{
		Rows:                    rows,
		Categories:              cats,
		Statuses:                statuses,
		HelpdeskRefreshInterval:   getSettingInt("helpdesk_refresh_interval", 300),
		UnassignedRefreshInterval: getSettingInt("unassigned_refresh_interval", 300),
	}, nil
}

// ---------------------------------------------------------------------------
// Subtask helpers
// ---------------------------------------------------------------------------

func parseSubtasks(raw string) []Subtask {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}

	lines := strings.Split(raw, "\n")
	subtasks := make([]Subtask, 0, len(lines))

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, "|", 2)
		completed := false
		text := line

		if len(parts) == 2 {
			completed = isCompletedValue(parts[0])
			text = strings.TrimSpace(parts[1])
		}

		if text == "" {
			continue
		}

		subtasks = append(subtasks, Subtask{Completed: completed, Text: text})
	}

	if len(subtasks) == 0 {
		return nil
	}
	return subtasks
}

func isCompletedValue(value string) bool {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "1", "true", "yes", "y", "checked", "done", "complete", "completed", "x":
		return true
	default:
		return false
	}
}

func hasSubtasks(raw string) bool {
	return len(parseSubtasks(raw)) > 0
}

func allSubtasksComplete(raw string) bool {
	subtasks := parseSubtasks(raw)
	if len(subtasks) == 0 {
		return false
	}
	for _, s := range subtasks {
		if !s.Completed {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Slug utilities
// ---------------------------------------------------------------------------

var nonAlnum = regexp.MustCompile(`[^a-z0-9]+`)

func slugify(name string) string {
	s := strings.ToLower(strings.TrimSpace(name))
	s = nonAlnum.ReplaceAllString(s, "-")
	s = strings.Trim(s, "-")
	return s
}

func uniqueSlug(base string) (string, error) {
	slug := base
	for i := 2; ; i++ {
		var exists bool
		err := db.QueryRow(
			`SELECT EXISTS(SELECT 1 FROM categories WHERE slug=$1 UNION SELECT 1 FROM statuses WHERE slug=$1)`,
			slug,
		).Scan(&exists)
		if err != nil {
			return "", err
		}
		if !exists {
			return slug, nil
		}
		slug = base + "-" + strconv.Itoa(i)
	}
}

// ---------------------------------------------------------------------------
// Page handlers
// ---------------------------------------------------------------------------

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}

	data, err := buildBoardData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if session, ok := r.Context().Value(contextKeyUser).(*SessionData); ok && session != nil {
		data.APIKey = getUserApiKey(session.UserID)
	}

	if err := tmpl.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// boardHandler renders only the board section (used by HTMX after settings changes)
func boardHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	data, err := buildBoardData()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if err := tmpl.ExecuteTemplate(w, "board_fragment.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// ---------------------------------------------------------------------------
// Card handlers
// ---------------------------------------------------------------------------

func createCardHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/card" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	title := r.FormValue("title")
	description := r.FormValue("description")
	subtasks := r.FormValue("subtasks")
	status := r.FormValue("status")
	category := r.FormValue("category")

	if !isValidStatus(status) {
		status = "todo"
	}
	if !isValidCategory(category) {
		category = "work"
	}

	if strings.TrimSpace(title) == "" && strings.TrimSpace(description) == "" && strings.TrimSpace(subtasks) == "" {
		http.Error(w, "Empty card not allowed", http.StatusBadRequest)
		return
	}

	var maxOrder int
	err := db.QueryRow(
		`SELECT COALESCE(MAX(card_order), 0) FROM cards WHERE category=$1 AND status=$2`,
		category, status,
	).Scan(&maxOrder)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	maxOrder++

	var newID int
	err = db.QueryRow(
		`INSERT INTO cards (title, description, subtasks, status, category, card_order) VALUES ($1, $2, $3, $4, $5, $6) RETURNING id`,
		title, description, subtasks, status, category, maxOrder,
	).Scan(&newID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	card := Card{
		ID: newID, Title: title, Description: description,
		Subtasks: subtasks, Status: status, Category: category, CardOrder: maxOrder,
	}

	broadcastBoardUpdate()

	if r.Header.Get("HX-Request") != "" {
		if err := tmpl.ExecuteTemplate(w, "card_fragment.html", card); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}

	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func cardRouter(w http.ResponseWriter, r *http.Request) {
	parts := strings.Split(r.URL.Path, "/")
	if len(parts) < 4 {
		http.NotFound(w, r)
		return
	}

	id, err := strconv.Atoi(parts[2])
	if err != nil {
		http.NotFound(w, r)
		return
	}

	action := parts[3]

	switch action {
	case "move":
		moveCardHandler(w, r, id)
	case "edit":
		editCardHandler(w, r, id)
	case "update":
		updateCardHandler(w, r, id)
	case "delete":
		deleteCardHandler(w, r, id)
	case "view":
		viewCardHandler(w, r, id)
	case "subtask":
		if len(parts) < 5 {
			http.NotFound(w, r)
			return
		}
		index, err := strconv.Atoi(parts[4])
		if err != nil {
			http.NotFound(w, r)
			return
		}
		toggleSubtaskHandler(w, r, id, index)
	default:
		http.NotFound(w, r)
	}
}

func moveCardHandler(w http.ResponseWriter, r *http.Request, id int) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	newStatus := r.FormValue("status")
	newCategory := r.FormValue("category")

	if !isValidStatus(newStatus) {
		http.Error(w, "Invalid status", http.StatusBadRequest)
		return
	}
	if !isValidCategory(newCategory) {
		http.Error(w, "Invalid category", http.StatusBadRequest)
		return
	}

	newOrder, err := strconv.Atoi(r.FormValue("order"))
	if err != nil {
		http.Error(w, "Invalid order", http.StatusBadRequest)
		return
	}

	_, err = db.Exec(`UPDATE cards SET category=$1, status=$2, card_order=$3 WHERE id=$4`, newCategory, newStatus, newOrder, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = db.Exec(`
		WITH OrderedCards AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY card_order, id) AS new_order
			FROM cards
			WHERE category = $1 AND status = $2
		)
		UPDATE cards SET card_order = OrderedCards.new_order
		FROM OrderedCards WHERE cards.id = OrderedCards.id
	`, newCategory, newStatus)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	broadcastBoardUpdate()

	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func editCardHandler(w http.ResponseWriter, r *http.Request, id int) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	card, err := getCardByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err := tmpl.ExecuteTemplate(w, "card_edit_fragment.html", card); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func updateCardHandler(w http.ResponseWriter, r *http.Request, id int) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	title := r.FormValue("title")
	description := r.FormValue("description")
	subtasks := r.FormValue("subtasks")
	dueDateStr := r.FormValue("due_on")

	var dueOn sql.NullTime
	if dueDateStr != "" {
		if t, err := time.Parse("2006-01-02", dueDateStr); err == nil {
			dueOn = sql.NullTime{Time: t, Valid: true}
		}
	}

	_, err := db.Exec(`UPDATE cards SET title=$1, description=$2, subtasks=$3, updated_at=NOW(), due_on=$4 WHERE id=$5`, title, description, subtasks, dueOn, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	updated, err := getCardByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err := tmpl.ExecuteTemplate(w, "card_fragment.html", updated); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func deleteCardHandler(w http.ResponseWriter, r *http.Request, id int) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	_, err := db.Exec(`DELETE FROM cards WHERE id=$1`, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

func viewCardHandler(w http.ResponseWriter, r *http.Request, id int) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	card, err := getCardByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	if err := tmpl.ExecuteTemplate(w, "card_fragment.html", card); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func toggleSubtaskHandler(w http.ResponseWriter, r *http.Request, id int, index int) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	card, err := getCardByID(id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}

	subtasks := parseSubtasks(card.Subtasks)
	if index < 0 || index >= len(subtasks) {
		http.Error(w, "Invalid subtask index", http.StatusBadRequest)
		return
	}

	subtasks[index].Completed = !subtasks[index].Completed

	lines := make([]string, len(subtasks))
	for i, s := range subtasks {
		completed := "0"
		if s.Completed {
			completed = "1"
		}
		lines[i] = completed + "|" + s.Text
	}

	newSubtasks := strings.Join(lines, "\n")
	if _, err := db.Exec(`UPDATE cards SET subtasks=$1 WHERE id=$2`, newSubtasks, id); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	card.Subtasks = newSubtasks
	if err := tmpl.ExecuteTemplate(w, "card_fragment.html", card); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func updateOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload OrderUpdatePayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if !isValidCategory(payload.Category) {
		http.Error(w, "Invalid category", http.StatusBadRequest)
		return
	}
	if !isValidStatus(payload.Status) {
		http.Error(w, "Invalid status", http.StatusBadRequest)
		return
	}

	for index, cardID := range payload.Order {
		_, err := db.Exec(
			`UPDATE cards SET category=$1, status=$2, card_order=$3 WHERE id=$4`,
			payload.Category, payload.Status, index+1, cardID,
		)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	_, err := db.Exec(`
		WITH OrderedCards AS (
			SELECT id, ROW_NUMBER() OVER (ORDER BY card_order, id) AS new_order
			FROM cards
			WHERE category = $1 AND status = $2
		)
		UPDATE cards SET card_order = OrderedCards.new_order
		FROM OrderedCards WHERE cards.id = OrderedCards.id
	`, payload.Category, payload.Status)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	broadcastBoardUpdate()

	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Category handlers
// ---------------------------------------------------------------------------

func createCategoryHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/category" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Error(w, "Name required", http.StatusBadRequest)
		return
	}

	base := slugify(name)
	if base == "" {
		http.Error(w, "Invalid name", http.StatusBadRequest)
		return
	}

	slug, err := uniqueSlug(base)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var maxOrder int
	if err := db.QueryRow(`SELECT COALESCE(MAX(row_order), 0) FROM categories`).Scan(&maxOrder); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	_, err = db.Exec(`INSERT INTO categories (name, slug, row_order, locked) VALUES ($1, $2, $3, false)`,
		name, slug, maxOrder+1)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if r.Header.Get("HX-Request") != "" {
		boardHandler(w, r)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func categoryRouter(w http.ResponseWriter, r *http.Request) {
	// /category/order  OR  /category/{slug}/{action}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	if len(parts) == 2 && parts[1] == "order" {
		reorderCategoriesHandler(w, r)
		return
	}

	if len(parts) < 3 {
		http.NotFound(w, r)
		return
	}

	slug := parts[1]
	action := parts[2]

	switch action {
	case "rename":
		renameCategoryHandler(w, r, slug)
	case "delete":
		deleteCategoryHandler(w, r, slug)
	default:
		http.NotFound(w, r)
	}
}

func renameCategoryHandler(w http.ResponseWriter, r *http.Request, slug string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Error(w, "Name required", http.StatusBadRequest)
		return
	}

	res, err := db.Exec(`UPDATE categories SET name=$1 WHERE slug=$2 AND locked=false`, name, slug)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		http.Error(w, "Category not found or locked", http.StatusBadRequest)
		return
	}

	if r.Header.Get("HX-Request") != "" {
		boardHandler(w, r)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func deleteCategoryHandler(w http.ResponseWriter, r *http.Request, slug string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	// Check locked
	var locked bool
	err := db.QueryRow(`SELECT locked FROM categories WHERE slug=$1`, slug).Scan(&locked)
	if err == sql.ErrNoRows {
		http.Error(w, "Category not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if locked {
		http.Error(w, "Category is locked", http.StatusBadRequest)
		return
	}

	// Count cards
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cards WHERE category=$1`, slug).Scan(&count); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	moveTo := strings.TrimSpace(r.FormValue("move_to"))

	if count > 0 {
		if moveTo == "" {
			// Return 409 with card count so frontend can prompt
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]int{"count": count})
			return
		}
		if !isValidCategory(moveTo) {
			http.Error(w, "Invalid move_to category", http.StatusBadRequest)
			return
		}
		if _, err := db.Exec(`UPDATE cards SET category=$1 WHERE category=$2`, moveTo, slug); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if _, err := db.Exec(`DELETE FROM categories WHERE slug=$1`, slug); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if r.Header.Get("HX-Request") != "" {
		boardHandler(w, r)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func reorderCategoriesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload SlugOrderPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	for i, slug := range payload.Order {
		if _, err := db.Exec(`UPDATE categories SET row_order=$1 WHERE slug=$2`, i+1, slug); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

// ---------------------------------------------------------------------------
// Status handlers
// ---------------------------------------------------------------------------

func createStatusHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/status" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Error(w, "Name required", http.StatusBadRequest)
		return
	}

	base := slugify(name)
	if base == "" {
		http.Error(w, "Invalid name", http.StatusBadRequest)
		return
	}

	slug, err := uniqueSlug(base)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	var maxOrder int
	if err := db.QueryRow(`SELECT COALESCE(MAX(col_order), 0) FROM statuses`).Scan(&maxOrder); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	// Insert before "done" — place at maxOrder (done is always last)
	_, err = db.Exec(`INSERT INTO statuses (name, slug, col_order, locked) VALUES ($1, $2, $3, false)`,
		name, slug, maxOrder)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if r.Header.Get("HX-Request") != "" {
		boardHandler(w, r)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func statusRouter(w http.ResponseWriter, r *http.Request) {
	// /status/order  OR  /status/{slug}/{action}
	parts := strings.Split(strings.Trim(r.URL.Path, "/"), "/")

	if len(parts) == 2 && parts[1] == "order" {
		reorderStatusesHandler(w, r)
		return
	}

	if len(parts) < 3 {
		http.NotFound(w, r)
		return
	}

	slug := parts[1]
	action := parts[2]

	switch action {
	case "rename":
		renameStatusHandler(w, r, slug)
	case "delete":
		deleteStatusHandler(w, r, slug)
	default:
		http.NotFound(w, r)
	}
}

func renameStatusHandler(w http.ResponseWriter, r *http.Request, slug string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		http.Error(w, "Name required", http.StatusBadRequest)
		return
	}

	res, err := db.Exec(`UPDATE statuses SET name=$1 WHERE slug=$2 AND locked=false`, name, slug)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if n, _ := res.RowsAffected(); n == 0 {
		http.Error(w, "Status not found or locked", http.StatusBadRequest)
		return
	}

	if r.Header.Get("HX-Request") != "" {
		boardHandler(w, r)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func deleteStatusHandler(w http.ResponseWriter, r *http.Request, slug string) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var locked bool
	err := db.QueryRow(`SELECT locked FROM statuses WHERE slug=$1`, slug).Scan(&locked)
	if err == sql.ErrNoRows {
		http.Error(w, "Status not found", http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if locked {
		http.Error(w, "Status is locked", http.StatusBadRequest)
		return
	}

	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM cards WHERE status=$1`, slug).Scan(&count); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	moveTo := strings.TrimSpace(r.FormValue("move_to"))

	if count > 0 {
		if moveTo == "" {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_ = json.NewEncoder(w).Encode(map[string]int{"count": count})
			return
		}
		if !isValidStatus(moveTo) {
			http.Error(w, "Invalid move_to status", http.StatusBadRequest)
			return
		}
		if _, err := db.Exec(`UPDATE cards SET status=$1 WHERE status=$2`, moveTo, slug); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if _, err := db.Exec(`DELETE FROM statuses WHERE slug=$1`, slug); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	if r.Header.Get("HX-Request") != "" {
		boardHandler(w, r)
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func reorderStatusesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var payload SlugOrderPayload
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	for i, slug := range payload.Order {
		if _, err := db.Exec(`UPDATE statuses SET col_order=$1 WHERE slug=$2`, i+1, slug); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}

	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
}

// ---------------------------------------------------------------------------
// JSON API endpoints (for MCP / external integrations)
// ---------------------------------------------------------------------------

func sseHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("X-Accel-Buffering", "no")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	ch := make(chan string, 1)
	sseMu.Lock()
	sseClients[ch] = struct{}{}
	sseMu.Unlock()

	defer func() {
		sseMu.Lock()
		delete(sseClients, ch)
		sseMu.Unlock()
	}()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.Context().Done():
			return
		case msg := <-ch:
			fmt.Fprintf(w, "event: %s\ndata: {}\n\n", msg)
			flusher.Flush()
		case <-ticker.C:
			fmt.Fprintf(w, ": keepalive\n\n")
			flusher.Flush()
		}
	}
}

// ---------------------------------------------------------------------------

func rotateApiKeyHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	session, ok := r.Context().Value(contextKeyUser).(*SessionData)
	if !ok || session == nil {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	newKey := generateApiKey()
	if _, err := db.Exec(`UPDATE users SET api_key=$1 WHERE id=$2`, newKey, session.UserID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"key": newKey})
}

func apiCardsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query(`SELECT id, title, description, subtasks, status, category, card_order, created_at, updated_at, due_on FROM cards ORDER BY category, status, card_order`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	cards := []Card{}
	for rows.Next() {
		var c Card
		if err := rows.Scan(&c.ID, &c.Title, &c.Description, &c.Subtasks, &c.Status, &c.Category, &c.CardOrder, &c.CreatedAt, &c.UpdatedAt, &c.DueOn); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		cards = append(cards, c)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cards)
}

func apiCategoriesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query(`SELECT id, name, slug, row_order, locked FROM categories ORDER BY row_order`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	cats := []Category{}
	for rows.Next() {
		var c Category
		if err := rows.Scan(&c.ID, &c.Name, &c.Slug, &c.RowOrder, &c.Locked); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		cats = append(cats, c)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(cats)
}

func apiStatusesHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	rows, err := db.Query(`SELECT id, name, slug, col_order, locked FROM statuses ORDER BY col_order`)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer rows.Close()

	statuses := []Status{}
	for rows.Next() {
		var s Status
		if err := rows.Scan(&s.ID, &s.Name, &s.Slug, &s.ColOrder, &s.Locked); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		statuses = append(statuses, s)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(statuses)
}

// ---------------------------------------------------------------------------
// Settings handler
// ---------------------------------------------------------------------------

func settingsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	if val := r.FormValue("helpdesk_refresh_interval"); val != "" {
		n, err := strconv.Atoi(val)
		if err != nil || n < 15 || n > 900 {
			http.Error(w, "Invalid value: must be 15–900", http.StatusBadRequest)
			return
		}
		if err := setSettingInt("helpdesk_refresh_interval", n); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if val := r.FormValue("unassigned_refresh_interval"); val != "" {
		n, err := strconv.Atoi(val)
		if err != nil || n < 15 || n > 900 {
			http.Error(w, "Invalid value: must be 15–900", http.StatusBadRequest)
			return
		}
		if err := setSettingInt("unassigned_refresh_interval", n); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// ---------------------------------------------------------------------------
// Freshservice / Helpdesk handlers
// ---------------------------------------------------------------------------

func fetchFreshserviceTickets() ([]FreshserviceTicket, error) {
	if fsAPIKey == "" || fsDomain == "" {
		return nil, fmt.Errorf("Freshservice not configured")
	}

	sixMonthsAgo := time.Now().AddDate(0, -6, 0).Format("2006-01-02")
	query := fmt.Sprintf(
		`(status:2 OR status:3 OR status:6 OR status:7 OR status:8) AND agent_id:33000703321 AND created_at:>'%s'`,
		sixMonthsAgo,
	)

	params := url.Values{}
	params.Set("query", `"`+query+`"`)
	params.Set("per_page", "100")
	apiURL := fmt.Sprintf("https://%s/api/v2/tickets/filter?%s", fsDomain, params.Encode())

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(fsAPIKey, "X")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Freshservice API returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var fsResp FreshserviceResponse
	if err := json.Unmarshal(body, &fsResp); err != nil {
		return nil, err
	}

	return fsResp.Tickets, nil
}

func helpdeskFragmentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	tickets, err := fetchFreshserviceTickets()
	if err != nil {
		log.Printf("Freshservice fetch error: %v", err)
		tickets = []FreshserviceTicket{}
	}

	statusCols := []struct {
		Name     string
		StatusID int
	}{
		{"Open", 2},
		{"Pending", 3},
		{"Pending Customer", 6},
		{"Pending Vendor", 7},
		{"On Hold", 8},
	}

	cols := make([]HelpdeskColumn, len(statusCols))
	for i, sc := range statusCols {
		cols[i] = HelpdeskColumn{Name: sc.Name}
		for _, t := range tickets {
			if t.Status == sc.StatusID {
				cols[i].Tickets = append(cols[i].Tickets, t)
			}
		}
	}

	data := struct {
		Columns []HelpdeskColumn
		Domain  string
	}{
		Columns: cols,
		Domain:  fsDomain,
	}

	if err := tmpl.ExecuteTemplate(w, "helpdesk_row.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func fetchUnassignedTickets() ([]FreshserviceTicket, error) {
	if fsAPIKey == "" || fsDomain == "" {
		return nil, fmt.Errorf("Freshservice not configured")
	}

	sixMonthsAgo := time.Now().AddDate(0, -6, 0).Format("2006-01-02")
	query := fmt.Sprintf(
		`(status:2 OR status:3 OR status:6 OR status:7) AND (group_id:33000158516 OR group_id:33000158515) AND agent_id:null AND created_at:>'%s'`,
		sixMonthsAgo,
	)

	params := url.Values{}
	params.Set("query", `"`+query+`"`)
	params.Set("per_page", "100")
	apiURL := fmt.Sprintf("https://%s/api/v2/tickets/filter?%s", fsDomain, params.Encode())

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(fsAPIKey, "X")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Freshservice API returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var fsResp FreshserviceResponse
	if err := json.Unmarshal(body, &fsResp); err != nil {
		return nil, err
	}

	return fsResp.Tickets, nil
}

func unassignedFragmentHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var cards []UnassignedCard

	// Unassigned tickets
	tickets, err := fetchUnassignedTickets()
	if err != nil {
		log.Printf("Unassigned tickets fetch error: %v", err)
	}
	for _, t := range tickets {
		cards = append(cards, UnassignedCard{
			TicketID: t.ID,
			Subject:  t.Subject,
			GroupID:  t.GroupID,
			DueBy:    t.DueBy,
			IsTask:   false,
		})
	}

	// Unassigned tasks from open tickets created in the last month
	taskTickets, err := fetchRecentOpenTickets()
	if err != nil {
		log.Printf("Task tickets fetch error: %v", err)
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, tt := range taskTickets {
		wg.Add(1)
		go func(ticketID int, subject string) {
			defer wg.Done()
			tasks, err := fetchTicketTasks(ticketID)
			if err != nil {
				log.Printf("Task fetch error for ticket %d: %v", ticketID, err)
				return
			}
			for _, task := range tasks {
				if task.Deleted {
					continue
				}
				if (task.Status == 1 || task.Status == 2) &&
					(task.GroupID == 33000158516 || task.GroupID == 33000158515) {
					mu.Lock()
					cards = append(cards, UnassignedCard{
						TicketID: ticketID,
						Subject:  subject,
						GroupID:  task.GroupID,
						DueBy:    task.DueDate,
						IsTask:   true,
					})
					mu.Unlock()
				}
			}
		}(tt.ID, tt.Subject)
	}
	wg.Wait()

	data := struct {
		Cards  []UnassignedCard
		Domain string
	}{
		Cards:  cards,
		Domain: fsDomain,
	}

	if err := tmpl.ExecuteTemplate(w, "unassigned_row.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func fetchRecentOpenTickets() ([]FreshserviceTicket, error) {
	if fsAPIKey == "" || fsDomain == "" {
		return nil, fmt.Errorf("Freshservice not configured")
	}

	oneMonthAgo := time.Now().AddDate(0, -1, 0).Format("2006-01-02")
	query := fmt.Sprintf(`status:2 AND created_at:>'%s'`, oneMonthAgo)

	params := url.Values{}
	params.Set("query", `"`+query+`"`)
	params.Set("per_page", "100")
	apiURL := fmt.Sprintf("https://%s/api/v2/tickets/filter?%s", fsDomain, params.Encode())

	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(fsAPIKey, "X")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Freshservice API returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var fsResp FreshserviceResponse
	if err := json.Unmarshal(body, &fsResp); err != nil {
		return nil, err
	}

	return fsResp.Tickets, nil
}

func fetchTicketTasks(ticketID int) ([]TicketTask, error) {
	if fsAPIKey == "" || fsDomain == "" {
		return nil, fmt.Errorf("Freshservice not configured")
	}

	apiURL := fmt.Sprintf("https://%s/api/v2/tickets/%d/tasks", fsDomain, ticketID)
	req, err := http.NewRequest("GET", apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(fsAPIKey, "X")
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Freshservice tasks API returned %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var tr TicketTasksResponse
	if err := json.Unmarshal(body, &tr); err != nil {
		return nil, err
	}

	return tr.Tasks, nil
}

// ---------------------------------------------------------------------------
// Auth helpers
// ---------------------------------------------------------------------------

func generatePassword(n int) string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		log.Fatalf("Failed to generate random password: %v", err)
	}
	for i := range b {
		b[i] = chars[int(b[i])%len(chars)]
	}
	return string(b)
}

func seedAdminUser() {
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&count); err != nil {
		log.Printf("Auth: could not count users: %v", err)
		return
	}
	if count > 0 {
		return
	}

	username := getEnv("ADMIN_USER", "admin")
	password := getEnv("ADMIN_PASS", "")
	if password == "" {
		password = generatePassword(8)
		log.Printf("AUTH SETUP: No ADMIN_PASS set and no users exist. Created user '%s' with password: %s", username, password)
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.DefaultCost)
	if err != nil {
		log.Fatalf("Auth: failed to hash admin password: %v", err)
	}

	if _, err := db.Exec(`INSERT INTO users (username, password_hash) VALUES ($1, $2)`, username, string(hash)); err != nil {
		log.Printf("Auth: failed to seed admin user: %v", err)
	} else {
		log.Printf("Auth: seeded admin user '%s'", username)
	}
}

func signSession(payload string) string {
	mac := hmac.New(sha256.New, sessionSecret)
	mac.Write([]byte(payload))
	return base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

func createSessionCookie(userID int, username string) (*http.Cookie, error) {
	data := SessionData{
		UserID:   userID,
		Username: username,
		Expires:  time.Now().Add(24 * time.Hour).Unix(),
	}
	jsonBytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	payload := base64.RawURLEncoding.EncodeToString(jsonBytes)
	sig := signSession(payload)
	value := payload + "." + sig

	return &http.Cookie{
		Name:     "kanban_session",
		Value:    value,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteLaxMode,
		MaxAge:   86400,
	}, nil
}

func validateSessionCookie(r *http.Request) *SessionData {
	cookie, err := r.Cookie("kanban_session")
	if err != nil {
		return nil
	}
	parts := strings.SplitN(cookie.Value, ".", 2)
	if len(parts) != 2 {
		return nil
	}
	payload, sig := parts[0], parts[1]
	if sig != signSession(payload) {
		return nil
	}
	jsonBytes, err := base64.RawURLEncoding.DecodeString(payload)
	if err != nil {
		return nil
	}
	var data SessionData
	if err := json.Unmarshal(jsonBytes, &data); err != nil {
		return nil
	}
	if time.Now().Unix() > data.Expires {
		return nil
	}
	return &data
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Public paths
		if r.URL.Path == "/login" || r.URL.Path == "/logout" || r.URL.Path == "/favicon.ico" {
			next.ServeHTTP(w, r)
			return
		}
		// API key bypass
		if key := r.Header.Get("X-API-Key"); key != "" {
			var userID int
			if err := db.QueryRow(`SELECT id FROM users WHERE api_key=$1`, key).Scan(&userID); err == nil {
				next.ServeHTTP(w, r)
				return
			}
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		session := validateSessionCookie(r)
		if session == nil {
			http.Redirect(w, r, "/login", http.StatusSeeOther)
			return
		}
		ctx := context.WithValue(r.Context(), contextKeyUser, session)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func loginHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodGet {
		// Already logged in? Redirect home.
		if validateSessionCookie(r) != nil {
			http.Redirect(w, r, "/", http.StatusSeeOther)
			return
		}
		tmpl.ExecuteTemplate(w, "login.html", nil)
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	username := strings.TrimSpace(r.FormValue("username"))
	password := r.FormValue("password")

	var user User
	err := db.QueryRow(`SELECT id, username, password_hash FROM users WHERE username=$1`, username).
		Scan(&user.ID, &user.Username, &user.PasswordHash)
	if err != nil {
		tmpl.ExecuteTemplate(w, "login.html", map[string]string{"Error": "Invalid username or password."})
		return
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(password)); err != nil {
		tmpl.ExecuteTemplate(w, "login.html", map[string]string{"Error": "Invalid username or password."})
		return
	}

	cookie, err := createSessionCookie(user.ID, user.Username)
	if err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
		return
	}
	http.SetCookie(w, cookie)
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

func logoutHandler(w http.ResponseWriter, r *http.Request) {
	http.SetCookie(w, &http.Cookie{
		Name:     "kanban_session",
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
	})
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}
