package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/go-sql-driver/mysql"
)

func rekeyAfterDelete(key string, deleteID string) string {
	sum := sha256.Sum256([]byte(key + "|after-delete|" + deleteID))
	return hex.EncodeToString(sum[:])
}

func rekeyTestItem(now time.Time) *MemoryVectorOutboxItem {
	return &MemoryVectorOutboxItem{
		ContractVersion: MemoryVectorOutboxContract,
		OperationKey:    strings.Repeat("c", 64), Operation: "upsert",
		ChatSessionID: "session", SourceRevision: "revision",
		DocumentID: "precise_memory:session:unit", DocumentJSON: `{"ID":"precise_memory:session:unit"}`,
		EmbeddingReady: false, RequiredSourceState: "active", Status: "needs_embedding",
		CreatedAt: now, UpdatedAt: now,
	}
}

// A unit retired (vector deleted) and desired again gets a fresh upsert on
// replay instead of failing as superseded by the delete.
func TestRefreshAfterDeleteQueuesAFreshUpsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	ctx := WithMemoryAdmissionVectorReplay(context.Background(), true, false)
	item := rekeyTestItem(now)
	rows := func() *sqlmock.Rows {
		return sqlmock.NewRows([]string{"id", "operation", "chat_session_id", "source_revision", "document_id", "required_source_state", "status", "lease_until", "lifecycle_state"}).
			AddRow(44, item.Operation, item.ChatSessionID, item.SourceRevision, item.DocumentID, item.RequiredSourceState, "completed", nil, "active")
	}
	mock.ExpectQuery("SELECT o.id, o.operation").WithArgs(item.OperationKey).WillReturnRows(rows())
	mock.ExpectQuery(`SELECT COUNT\(\*\)`).WithArgs(item.ChatSessionID, item.DocumentID, int64(44)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT MAX\(id\)`).WithArgs(item.ChatSessionID, item.DocumentID, int64(44)).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(50))
	fresh := rekeyAfterDelete(item.OperationKey, "50")
	mock.ExpectQuery("SELECT o.id, o.operation").WithArgs(fresh).WillReturnError(sql.ErrNoRows)
	mock.ExpectExec("INSERT INTO memory_vector_outbox").
		WithArgs(MemoryVectorOutboxContract, fresh, "upsert", "session", "revision", item.DocumentID, item.DocumentJSON, false, "active", "needs_embedding", 0, nil, nil, nil, nil, now, now).
		WillReturnResult(sqlmock.NewResult(51, 1))
	inserted, err := enqueueAdmissionVectorOperation(ctx, db, item)
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	if item.OperationKey != strings.Repeat("c", 64) {
		t.Fatal("the caller's item must keep its key")
	}

	// A newer upsert (not a delete) still supersedes the refresh.
	mock.ExpectQuery("SELECT o.id, o.operation").WithArgs(item.OperationKey).WillReturnRows(rows())
	mock.ExpectQuery(`SELECT COUNT\(\*\)`).WithArgs(item.ChatSessionID, item.DocumentID, int64(44)).WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT MAX\(id\)`).WithArgs(item.ChatSessionID, item.DocumentID, int64(44)).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(nil))
	if _, err := enqueueAdmissionVectorOperation(ctx, db, item); err == nil || !strings.Contains(err.Error(), "superseded") {
		t.Fatalf("err = %v, want superseded", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

// Ordinary admission of a reactivated unit finds its old completed upsert; a
// delete after it means the vector is gone, so a fresh upsert is queued.
func TestEnqueueAfterDeleteQueuesAFreshUpsert(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	now := time.Date(2026, 11, 20, 12, 0, 0, 0, time.UTC)
	item := rekeyTestItem(now)
	mock.ExpectExec("INSERT INTO memory_vector_outbox").WillReturnError(&mysql.MySQLError{Number: 1062, Message: "Duplicate entry"})
	mock.ExpectQuery("SELECT operation, chat_session_id, source_revision, document_id").WithArgs(item.OperationKey).
		WillReturnRows(sqlmock.NewRows([]string{"operation", "chat_session_id", "source_revision", "document_id", "document_json", "embedding_ready", "required_source_state", "id"}).
			AddRow(item.Operation, item.ChatSessionID, item.SourceRevision, item.DocumentID, item.DocumentJSON, item.EmbeddingReady, item.RequiredSourceState, 7))
	mock.ExpectQuery(`SELECT MAX\(id\)`).WithArgs(item.ChatSessionID, item.DocumentID, int64(7)).WillReturnRows(sqlmock.NewRows([]string{"max"}).AddRow(9))
	fresh := rekeyAfterDelete(item.OperationKey, "9")
	mock.ExpectExec("INSERT INTO memory_vector_outbox").
		WithArgs(MemoryVectorOutboxContract, fresh, "upsert", "session", "revision", item.DocumentID, item.DocumentJSON, false, "active", "needs_embedding", 0, nil, nil, nil, nil, now, now).
		WillReturnResult(sqlmock.NewResult(10, 1))
	inserted, err := enqueueMemoryVectorOperation(context.Background(), db, item)
	if err != nil || !inserted {
		t.Fatalf("inserted=%v err=%v", inserted, err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
