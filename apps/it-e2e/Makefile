# E2E for the embedded query UI (backend/apps/ui served at /ui).
#
#   make query-ui          # full cycle: stack up → seed → playwright → down
#   make query-ui-test     # tests only, against an already-running stack
#
# HEADED=1 opens a visible browser with slow motion.

BACKEND := ../backend

.PHONY: query-ui query-ui-test query-ui-up query-ui-down

query-ui: query-ui-up
	@status=0; $(MAKE) query-ui-test || status=$$?; \
	$(MAKE) query-ui-down; \
	exit $$status

query-ui-up:
	cd $(BACKEND) && docker compose down -v --remove-orphans
	cd $(BACKEND) && docker compose up --build -d
	cd $(BACKEND) && go run ./tools/ui-seed

query-ui-test:
	npx playwright test -c playwright.query.config.ts

query-ui-down:
	cd $(BACKEND) && docker compose down -v --remove-orphans
