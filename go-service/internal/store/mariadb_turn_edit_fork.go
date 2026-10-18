package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"strings"
	"time"
)

// syncUnderivedSourceTextTx keeps a turn's source revision on the edited chat
// text while nothing has been derived from it yet. A turn waiting for the
// memory worker (its first Critic run failed, say) is derived from the source
// revision, not from chat_logs, so an edit made in the meantime would
// otherwise be lost and leave the two texts apart. Once a result exists,
// committed or staged, the source keeps the text that result was grounded in.
func syncUnderivedSourceTextTx(ctx context.Context, tx *sql.Tx, sid string, t int, edit HistoricalTurnEdit, now time.Time) error {
	content := strings.TrimSpace(strings.Join([]string{edit.UserContent, edit.AssistantContent}, "\n"))
	_, err := tx.ExecContext(ctx, `
		UPDATE memory_source_revisions
		SET raw_user_content = ?, raw_assistant_content = ?, combined_content_hash = ?, updated_at = ?
		WHERE chat_session_id = ? AND turn_index = ? AND lifecycle_state = 'active'
		  AND derived_admission_state <> 'committed'
		  AND (derived_result_json IS NULL OR derived_result_json = '')
	`, edit.UserContent, edit.AssistantContent, fmt.Sprintf("%x", sha256.Sum256([]byte(content))), now, sid, t)
	return err
}
