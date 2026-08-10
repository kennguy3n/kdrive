-- Composite index for CheckChunkHashes query pattern.
-- The CheckChunkHashes method queries:
--   WHERE content_id = $1 AND chunk_content_hash = ANY($2)
-- The existing single-column index on chunk_content_hash forces a filter
-- across all content_ids. This composite index enables an efficient index-only
-- scan scoped to the specific content_id.

CREATE INDEX IF NOT EXISTS content_chunks_content_hash_idx
    ON content_chunks(content_id, chunk_content_hash);

INSERT INTO schema_version (version) VALUES (6)
    ON CONFLICT (version) DO NOTHING;
