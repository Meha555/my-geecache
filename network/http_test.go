package network

import (
	"fmt"
	"geecache"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

var db = map[string]string{
	"Tom":  "630",
	"Jack": "589",
	"Sam":  "567",
}

func TestHTTPPool(t *testing.T) {
	geecache.NewGroup("scores", 2<<10, geecache.GetterFunc(func(key string) ([]byte, error) {
		log.Println("[SlowDB] search key", key)
		if v, ok := db[key]; ok {
			return []byte(v), nil
		}
		return nil, fmt.Errorf("%v is not exist", key)
	}))
	pool := NewHTTPPool(":9999")

	t.Run("Test GET /scores/Tom", func(t *testing.T) {
		url, _ := url.JoinPath(defaultBasePath, "/scores/Tom")
		req := httptest.NewRequest(http.MethodGet, url, nil)
		recorder := httptest.NewRecorder()

		pool.ServeHTTP(recorder, req)

		if recorder.Code != http.StatusOK {
			t.Errorf("Expected status code %d, got %d", http.StatusOK, recorder.Code)
		}

		expectedBody := "630"
		body := recorder.Body.String()
		if body != expectedBody {
			t.Errorf("Expected body %s, got %s", expectedBody, body)
		}
	})
}
