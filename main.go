package main

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"golang.org/x/crypto/bcrypt"
)

const (
	StatusTodo       = "todo"
	StatusInProgress = "inprogress"
	StatusDone       = "done"
)

type Card struct {
	ID          int
	Title       string
	Description string
	Subtasks    string
	Status      string // "todo", "inprogress", "done"
	CardOrder   int
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

var db *sql.DB
var tmpl *template.Template
var sessionSecret []byte

type OrderUpdatePayload struct {
	Status string `json:"status"`
	Order  []int  `json:"order"`
}

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

	// Create cards table if it doesn't exist.
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS cards (
		id          SERIAL PRIMARY KEY,
		title       TEXT NOT NULL,
		description TEXT NOT NULL DEFAULT '',
		subtasks    TEXT NOT NULL DEFAULT '',
		status      VARCHAR(50) NOT NULL DEFAULT 'todo',
		card_order  INTEGER NOT NULL DEFAULT 0
	)`)
	if err != nil {
		log.Fatalf("Failed to create cards table: %v", err)
	}

	// Create users table if it doesn't exist.
	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS users (
		id            SERIAL PRIMARY KEY,
		username      VARCHAR(50) UNIQUE NOT NULL,
		password_hash VARCHAR(255) NOT NULL,
		created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
		updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
	)`)
	if err != nil {
		log.Fatalf("Failed to create users table: %v", err)
	}

	// Session secret — set SESSION_SECRET env var for persistence across restarts.
	secret := getEnv("SESSION_SECRET", "")
	if secret == "" {
		log.Println("WARNING: SESSION_SECRET not set — sessions will be invalidated on restart. Set SESSION_SECRET in your environment.")
		sessionSecret = make([]byte, 32)
		if _, err := rand.Read(sessionSecret); err != nil {
			log.Fatalf("Failed to generate session secret: %v", err)
		}
	} else {
		sessionSecret = []byte(secret)
	}

	seedAdminUser()

	funcMap := template.FuncMap{
		"split": func(s, sep string) []string {
			s = strings.TrimSpace(s)
			if s == "" {
				return nil
			}
			return strings.Split(s, sep)
		},
		"trim": strings.TrimSpace,
	}
	tmpl = template.Must(template.New("").Funcs(funcMap).ParseGlob("templates/*.html"))

	// Favicon handler to avoid 404.
	http.HandleFunc("/favicon.ico", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	http.HandleFunc("/login", loginHandler)
	http.HandleFunc("/logout", logoutHandler)
	http.HandleFunc("/", indexHandler)
	http.HandleFunc("/card", createCardHandler)
	http.HandleFunc("/card/", cardRouter)
	http.HandleFunc("/card/order", updateOrderHandler)

	serverPort := getEnv("SERVER_PORT", "17808")
	log.Println("Server started on :" + serverPort)
	log.Fatal(http.ListenAndServe(":"+serverPort, authMiddleware(http.DefaultServeMux)))
}

func getEnv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func getCardByID(id int) (*Card, error) {
	var card Card
	err := db.QueryRow("SELECT id, title, description, subtasks, status, card_order FROM cards WHERE id=$1", id).
		Scan(&card.ID, &card.Title, &card.Description, &card.Subtasks, &card.Status, &card.CardOrder)
	if err != nil {
		return nil, err
	}
	return &card, nil
}

func indexHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	cardsByStatus := map[string][]Card{
		StatusTodo:       {},
		StatusInProgress: {},
		StatusDone:       {},
	}
	rows, err := db.Query("SELECT id, title, description, subtasks, status, card_order FROM cards ORDER BY status, card_order")
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer func() {
		if err := rows.Close(); err != nil {
			log.Printf("Error closing rows: %v", err)
		}
	}()
	for rows.Next() {
		var c Card
		if err := rows.Scan(&c.ID, &c.Title, &c.Description, &c.Subtasks, &c.Status, &c.CardOrder); err != nil {
			continue
		}
		cardsByStatus[c.Status] = append(cardsByStatus[c.Status], c)
	}
	if err := tmpl.ExecuteTemplate(w, "index.html", cardsByStatus); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func createCardHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/card" || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	title := r.FormValue("title")
	description := r.FormValue("description")
	subtasks := r.FormValue("subtasks")
	status := r.FormValue("status")

	if status != StatusTodo && status != StatusInProgress && status != StatusDone {
		status = StatusTodo // Default to todo
	}

	if strings.TrimSpace(title) == "" && strings.TrimSpace(description) == "" && strings.TrimSpace(subtasks) == "" {
		http.Error(w, "Empty card not allowed", http.StatusBadRequest)
		return
	}
	var maxOrder int
	err := db.QueryRow("SELECT COALESCE(MAX(card_order), 0) FROM cards WHERE status=$1", status).Scan(&maxOrder)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	maxOrder++
	var newID int
	err = db.QueryRow("INSERT INTO cards (title, description, subtasks, status, card_order) VALUES ($1, $2, $3, $4, $5) RETURNING id",
		title, description, subtasks, status, maxOrder).Scan(&newID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	card := Card{ID: newID, Title: title, Description: description, Subtasks: subtasks, Status: status, CardOrder: maxOrder}
	if r.Header.Get("HX-Request") != "" {
		if err := tmpl.ExecuteTemplate(w, "card_fragment.html", card); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
		return
	}
	http.Redirect(w, r, "/", http.StatusSeeOther)
}

// cardRouter dispatches requests based on URL segments: /card/{id}/{action}
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

	if newStatus != StatusTodo && newStatus != StatusInProgress && newStatus != StatusDone {
		http.Error(w, "Invalid status", http.StatusBadRequest)
		return
	}

	newOrder, err := strconv.Atoi(r.FormValue("order"))
	if err != nil {
		http.Error(w, "Invalid order", http.StatusBadRequest)
		return
	}
	_, err = db.Exec("UPDATE cards SET status=$1, card_order=$2 WHERE id=$3", newStatus, newOrder, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	// Recalculate order for the destination lane.
	_, err = db.Exec(`
        WITH OrderedCards AS (
            SELECT id, ROW_NUMBER() OVER (ORDER BY card_order, id) AS new_order
            FROM cards
            WHERE status = $1
        )
        UPDATE cards SET card_order = OrderedCards.new_order
        FROM OrderedCards
        WHERE cards.id = OrderedCards.id;
    `, newStatus)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
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
	_, err := db.Exec("UPDATE cards SET title=$1, description=$2, subtasks=$3 WHERE id=$4", title, description, subtasks, id)
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
	_, err := db.Exec("DELETE FROM cards WHERE id=$1", id)
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

func updateOrderHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}
	var payload OrderUpdatePayload
	err := json.NewDecoder(r.Body).Decode(&payload)
	if err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	for index, cardId := range payload.Order {
		_, err := db.Exec("UPDATE cards SET status=$1, card_order=$2 WHERE id=$3", payload.Status, index+1, cardId)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}
	if _, err := w.Write([]byte("OK")); err != nil {
		log.Printf("Error writing response: %v", err)
	}
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
