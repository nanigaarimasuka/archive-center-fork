package httpapi

import (
	"context"
	"testing"

	"github.com/risulongmemory/archive-center-go/internal/config"
	"github.com/risulongmemory/archive-center-go/internal/store"
	"github.com/risulongmemory/archive-center-go/internal/vector"
)

// "원문만 반영하기" changes the stored chat text but keeps what was derived
// from the source revision. Session normalize must skip such a turn as
// complete, not fail it as a raw mismatch; an underived turn still fails.
func TestAdminRescanSkipsTextOnlyEditedTurnThatIsAlreadyDerived(t *testing.T) {
	for _, derived := range []bool{true, false} {
		base := newAdminDuplicateReprocessingStore()
		extraction := map[string]any{"turn_summary": "Mina finds the brass key under the desk."}
		source := base.source
		source.AssistantContent = "Mina finds the copper key under the desk."
		if derived {
			source.DerivedAdmissionState = "committed"
			source.DerivedAdmissionVersion = store.MemoryAdmissionContract
			source.DerivedExtractorVersion = completeTurnCriticPipelineVersion
			source.DerivedIndexVersion = memoryAdmissionIndexVersion
			source.DerivedResultJSON = mustCompactJSON(normalizePreciseMemoryValue(extraction))
			source.DerivedResultHash = memoryAdmissionResultHash(source.SourceRevision, extraction, store.MemoryAdmissionContract, completeTurnCriticPipelineVersion, memoryAdmissionIndexVersion)
			base.auditLogs = append(base.auditLogs, store.AuditLog{
				ChatSessionID: source.ChatSessionID, EventType: "critic_ingest_trace", TargetType: "turn", TargetID: int64(source.TurnIndex),
				DetailsJSON: mustCompactJSON(map[string]any{
					"pipeline_complete": true, "source_revision": source.SourceRevision,
					"derivation_version": store.MemoryAdmissionContract, "extractor_version": completeTurnCriticPipelineVersion, "index_version": memoryAdmissionIndexVersion,
				}),
			})
		}
		st := &adminCanonicalRawReprocessingStore{adminDuplicateReprocessingStore: base, registered: []store.MemorySourceRevision{source}}
		srv := &Server{Cfg: config.Default(), Store: st, Vector: vector.NewFakeVectorStore()}
		response, err := srv.runAdminRescan(context.Background(), source.ChatSessionID, adminRescanRequest{TurnIndices: []int{source.TurnIndex}, CanonicalRawReplay: true})
		if err != nil {
			t.Fatal(err)
		}
		if derived {
			skipped := response["skipped_turns"].([]map[string]any)
			if response["status"] != "ok" || intFromAny(response["failed"], -1) != 0 || len(skipped) != 1 || skipped[0]["reason"] != "derived_projection_complete" || len(st.admissions) != 0 {
				t.Fatalf("derived: response=%+v admissions=%d", response, len(st.admissions))
			}
			continue
		}
		failed := response["failed_turns"].([]map[string]any)
		if len(failed) != 1 || failed[0]["reason"] != "active_source_raw_mismatch" {
			t.Fatalf("underived: response=%+v", response)
		}
	}
}
