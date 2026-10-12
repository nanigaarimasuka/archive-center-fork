package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"strconv"
)

// memoryVectorUpsertKeyAfterDelete gives an upsert a new operation key when
// its document was deleted after the outbox row that already holds its key.
// Precise memory keys do not change with content, so a unit retired (its
// vector deleted) and later desired again would otherwise find its old,
// completed upsert and never write the vector back. The new key is derived
// from the latest such delete, so replaying the same state finds the same
// row. It returns "" when no delete came after existingID.
func memoryVectorUpsertKeyAfterDelete(ctx context.Context, exec memoryDerivationSQLExecutor, item *MemoryVectorOutboxItem, existingID int64) (string, error) {
	if item == nil || item.Operation != "upsert" {
		return "", nil
	}
	var deleteID sql.NullInt64
	if err := exec.QueryRowContext(ctx, `
		SELECT MAX(id)
		FROM memory_vector_outbox
		WHERE chat_session_id = ? AND document_id = ? AND operation = 'delete' AND id > ?
	`, item.ChatSessionID, item.DocumentID, existingID).Scan(&deleteID); err != nil {
		return "", err
	}
	if !deleteID.Valid {
		return "", nil
	}
	sum := sha256.Sum256([]byte(item.OperationKey + "|after-delete|" + strconv.FormatInt(deleteID.Int64, 10)))
	return hex.EncodeToString(sum[:]), nil
}
