// kanban-mcp is an MCP server (Streamable HTTP transport) that lets AI
// assistants manage the kanban board through its JSON API.
//
// Configuration (environment variables):
//
//	KANBAN_URL      base URL of the kanban app, e.g. http://kanban:17808 (required)
//	KANBAN_API_KEY  X-API-Key value from the kanban settings panel (required)
//	MCP_AUTH_TOKEN  bearer token MCP clients must present (required)
//	MCP_PORT        listen port (default 17809)
package main

import (
	"bytes"
	"context"
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ---------------------------------------------------------------------------
// Kanban API client
// ---------------------------------------------------------------------------

type kanbanClient struct {
	baseURL string
	apiKey  string
	http    *http.Client
}

func (k *kanbanClient) do(ctx context.Context, method, path string, body any) ([]byte, error) {
	var reader io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("encoding request body: %w", err)
		}
		reader = bytes.NewReader(b)
	}

	req, err := http.NewRequestWithContext(ctx, method, k.baseURL+path, reader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-API-Key", k.apiKey)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	resp, err := k.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("kanban API unreachable: %w", err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("kanban API %s %s returned %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(data)))
	}
	return data, nil
}

// ---------------------------------------------------------------------------
// Tool inputs
// ---------------------------------------------------------------------------

type listCardsInput struct {
	Category string `json:"category,omitempty" jsonschema:"optional category slug to filter by"`
	Status   string `json:"status,omitempty" jsonschema:"optional status slug to filter by"`
}

type createCardInput struct {
	Title       string `json:"title" jsonschema:"card title"`
	Description string `json:"description,omitempty" jsonschema:"card description (optional)"`
	Subtasks    string `json:"subtasks,omitempty" jsonschema:"subtasks, one per line in the form '0|task text' (0=open, 1=done)"`
	Status      string `json:"status,omitempty" jsonschema:"status slug (defaults to 'todo'); use list_statuses for valid slugs"`
	Category    string `json:"category,omitempty" jsonschema:"category slug (defaults to 'work'); use list_categories for valid slugs"`
	DueOn       string `json:"due_on,omitempty" jsonschema:"due date YYYY-MM-DD (optional)"`
}

type updateCardInput struct {
	ID          int     `json:"id" jsonschema:"card id"`
	Title       *string `json:"title,omitempty" jsonschema:"new title (omit to keep current)"`
	Description *string `json:"description,omitempty" jsonschema:"new description (omit to keep current)"`
	Subtasks    *string `json:"subtasks,omitempty" jsonschema:"new subtasks, one per line in the form '0|task text' (omit to keep current)"`
	Status      *string `json:"status,omitempty" jsonschema:"new status slug (omit to keep current)"`
	Category    *string `json:"category,omitempty" jsonschema:"new category slug (omit to keep current)"`
	CardOrder   *int    `json:"card_order,omitempty" jsonschema:"new position within the column, 1-based (omit to keep current)"`
	DueOn       *string `json:"due_on,omitempty" jsonschema:"new due date YYYY-MM-DD, empty string clears it (omit to keep current)"`
}

type moveCardInput struct {
	ID        int    `json:"id" jsonschema:"card id"`
	Status    string `json:"status" jsonschema:"destination status slug; use list_statuses for valid slugs"`
	Category  string `json:"category,omitempty" jsonschema:"destination category slug (omit to keep current)"`
	CardOrder *int   `json:"card_order,omitempty" jsonschema:"position within the destination column, 1-based (omit to place at the end)"`
}

type deleteCardInput struct {
	ID int `json:"id" jsonschema:"card id"`
}

type emptyInput struct{}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

func textResult(data []byte) *mcp.CallToolResult {
	return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: string(data)}}}
}

func buildServer(kc *kanbanClient) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "kanban", Version: "1.0.0"}, nil)

	mcp.AddTool(server, &mcp.Tool{
		Name: "list_cards",
		Description: "List kanban cards as JSON, optionally filtered by category and/or status slug. " +
			"Cards have fields ID, Title, Description, Subtasks, Status, Category, CardOrder, CreatedAt, UpdatedAt, DueOn.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in listCardsInput) (*mcp.CallToolResult, any, error) {
		data, err := kc.do(ctx, http.MethodGet, "/api/cards", nil)
		if err != nil {
			return nil, nil, err
		}
		if in.Category == "" && in.Status == "" {
			return textResult(data), nil, nil
		}
		var cards []map[string]any
		if err := json.Unmarshal(data, &cards); err != nil {
			return nil, nil, fmt.Errorf("decoding cards: %w", err)
		}
		filtered := []map[string]any{}
		for _, c := range cards {
			if in.Category != "" && c["Category"] != in.Category {
				continue
			}
			if in.Status != "" && c["Status"] != in.Status {
				continue
			}
			filtered = append(filtered, c)
		}
		out, err := json.Marshal(filtered)
		if err != nil {
			return nil, nil, err
		}
		return textResult(out), nil, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_categories",
		Description: "List the board's categories (rows) as JSON. Use the Slug values when creating or moving cards.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in emptyInput) (*mcp.CallToolResult, any, error) {
		data, err := kc.do(ctx, http.MethodGet, "/api/categories", nil)
		if err != nil {
			return nil, nil, err
		}
		return textResult(data), nil, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "list_statuses",
		Description: "List the board's statuses (columns) as JSON. Use the Slug values when creating or moving cards.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in emptyInput) (*mcp.CallToolResult, any, error) {
		data, err := kc.do(ctx, http.MethodGet, "/api/statuses", nil)
		if err != nil {
			return nil, nil, err
		}
		return textResult(data), nil, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "create_card",
		Description: "Create a new card on the kanban board. Returns the created card as JSON.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in createCardInput) (*mcp.CallToolResult, any, error) {
		body := map[string]any{"title": in.Title}
		if in.Description != "" {
			body["description"] = in.Description
		}
		if in.Subtasks != "" {
			body["subtasks"] = in.Subtasks
		}
		if in.Status != "" {
			body["status"] = in.Status
		}
		if in.Category != "" {
			body["category"] = in.Category
		}
		if in.DueOn != "" {
			body["due_on"] = in.DueOn
		}
		data, err := kc.do(ctx, http.MethodPost, "/api/cards", body)
		if err != nil {
			return nil, nil, err
		}
		return textResult(data), nil, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "update_card",
		Description: "Update fields of an existing card. Only the provided fields change. Returns the updated card as JSON.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in updateCardInput) (*mcp.CallToolResult, any, error) {
		body := map[string]any{}
		if in.Title != nil {
			body["title"] = *in.Title
		}
		if in.Description != nil {
			body["description"] = *in.Description
		}
		if in.Subtasks != nil {
			body["subtasks"] = *in.Subtasks
		}
		if in.Status != nil {
			body["status"] = *in.Status
		}
		if in.Category != nil {
			body["category"] = *in.Category
		}
		if in.CardOrder != nil {
			body["card_order"] = *in.CardOrder
		}
		if in.DueOn != nil {
			body["due_on"] = *in.DueOn
		}
		if len(body) == 0 {
			return nil, nil, fmt.Errorf("no fields to update were provided")
		}
		data, err := kc.do(ctx, http.MethodPatch, fmt.Sprintf("/api/cards/%d", in.ID), body)
		if err != nil {
			return nil, nil, err
		}
		return textResult(data), nil, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "move_card",
		Description: "Move a card to another status column (and optionally another category row / position). Returns the updated card as JSON.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in moveCardInput) (*mcp.CallToolResult, any, error) {
		body := map[string]any{"status": in.Status}
		if in.Category != "" {
			body["category"] = in.Category
		}
		if in.CardOrder != nil {
			body["card_order"] = *in.CardOrder
		}
		data, err := kc.do(ctx, http.MethodPatch, fmt.Sprintf("/api/cards/%d", in.ID), body)
		if err != nil {
			return nil, nil, err
		}
		return textResult(data), nil, nil
	})

	mcp.AddTool(server, &mcp.Tool{
		Name:        "delete_card",
		Description: "Permanently delete a card from the board.",
	}, func(ctx context.Context, req *mcp.CallToolRequest, in deleteCardInput) (*mcp.CallToolResult, any, error) {
		data, err := kc.do(ctx, http.MethodDelete, fmt.Sprintf("/api/cards/%d", in.ID), nil)
		if err != nil {
			return nil, nil, err
		}
		return textResult(data), nil, nil
	})

	return server
}

// ---------------------------------------------------------------------------
// Auth + startup
// ---------------------------------------------------------------------------

func requireBearer(token string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("Authorization")
		const prefix = "Bearer "
		if !strings.HasPrefix(auth, prefix) ||
			subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(auth, prefix)), []byte(token)) != 1 {
			http.Error(w, "Unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func mustEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		log.Fatalf("Required environment variable %s is not set", key)
	}
	return v
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func main() {
	kc := &kanbanClient{
		baseURL: strings.TrimRight(mustEnv("KANBAN_URL"), "/"),
		apiKey:  mustEnv("KANBAN_API_KEY"),
		http:    &http.Client{Timeout: 30 * time.Second},
	}
	authToken := mustEnv("MCP_AUTH_TOKEN")
	port := getEnv("MCP_PORT", "17809")

	server := buildServer(kc)
	handler := mcp.NewStreamableHTTPHandler(func(r *http.Request) *mcp.Server { return server }, nil)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		if _, err := w.Write([]byte("ok")); err != nil {
			log.Printf("Error writing health response: %v", err)
		}
	})
	mux.Handle("/", requireBearer(authToken, handler))

	log.Println("Kanban MCP server listening on :" + port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}
