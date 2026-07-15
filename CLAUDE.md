# CLAUDE.md

Fabric workspace DEV_Duckrun is git-synced with root folder set to `fabric/` — this is a
Fabric portal setting, not captured anywhere in the repo. The synced branch is whatever
feature branch is currently being worked on (switched in the portal); there is no shared
`dev` branch. Promotion: PR feature → `test` → `prod` (deployed by the ADO pipeline);
`main` carries the released template state.
