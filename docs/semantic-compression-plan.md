# Semantic Compression Implementation Plan

Goal: delete old mechanisms, not just wrap them.

## Passes

1. Runtime file specs are the handler table.
   - Delete the copied `HandlerSpec` concept.
   - Make staging, planning, preview, upload, and table creation read the same
     handled spec rows.

2. Archive state is facts-to-plan.
   - Derive state from source presence, scan generation, handler generation,
     file metadata, and artifact manifests.
   - Remove duplicate selectors and state branches that can disagree.

3. Extraction is one zip traversal.
   - Use one explicit extraction path for production staging and fixture unzip.
   - Express full extraction vs active-file extraction as data.

4. Stop where compression stops deleting.
   - Keep parser-specific code local unless a shared parser deletes multiple
     parser bodies.
   - Avoid repositories, managers, frameworks, and fake generic upserts.

Completion rule: production code and the total patch must both end net-negative.
