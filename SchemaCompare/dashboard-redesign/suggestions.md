# Dashboard redesign — Azure SQL Migration Manager (SchemaCompare)

Implementation spec for redesigning the dashboard of the web app. A static HTML reference
mockup lives alongside this file at `docs/dashboard-redesign/mockup.html` — open it in a
browser to see the target layout, spacing, and colours. All paths below are relative to the
repo root.

## Context

- App: React + TypeScript + Vite, react-router, plain CSS (no component library).
- Entry/routing: `apps/web/src/App.tsx`
- Current dashboard: `apps/web/src/pages/DashboardPage.tsx`
- Styles: `apps/web/src/styles.css` (CSS variables defined in `:root` — reuse them)
- API client: `apps/web/src/api.ts` (`apiFetch<T>(path)`)
- Types: `apps/web/src/types.ts`

The current dashboard is a single grid of environment cards (name, server, current version,
pending/applied counts, drift badge). It has no entry points to the app's tools, no aggregate
health view, and no help content.

**Important pre-existing bug to fix as part of this work:** three fully built pages are
unreachable — `apps/web/src/pages/PlanExplorerPage.tsx`, `apps/web/src/pages/RunsPage.tsx`,
and `apps/web/src/pages/RepoPage.tsx` have no routes in `App.tsx` and no nav links.

## Goals

1. Make the dashboard the hub of the app: health summary + launchpad for every tool.
2. Surface the three orphaned tool pages (Plan Explorer, Runs, Script Repository).
3. Add self-help guides so new users can onboard without external docs.
4. Harden the data fetching (partial failures, empty states, loading states, refresh).

## Layout (top to bottom — see mockup.html)

### 1. Summary strip

Four metric tiles in a responsive grid (`repeat(auto-fit, minmax(190px, 1fr))`):

| Tile | Value | Source |
|------|-------|--------|
| Environments | count | `GET /environments` |
| Pending migrations | sum of `pendingCount` across environments | `GET /environments/:id/status` (already fetched per env) |
| Drift detected | count of environments with `driftDetected === true`; tile uses danger styling when > 0, neutral when 0 | same as above |
| Last run | status of most recent run (e.g. "Passed" / "Failed") | `GET /runs` (first item) |

- Tiles are not links, except "Drift detected" which may link to the first drifted
  environment, and "Last run" which links to the Runs page.
- A "Refreshed N min ago" indicator with a manual refresh button sits in the page header.

### 2. Tool launcher cards

Section titled "Tools". One card per tool in a responsive grid
(`repeat(auto-fit, minmax(200px, 1fr))`). Each card: icon in a tinted rounded square,
title, one-line description, "Open →" affordance. Whole card is the link.

| Tool | Route | Description copy |
|------|-------|------------------|
| Schema Compare | `/schema-compare` | Diff two environments and generate sync scripts for tables, indexes, and constraints. |
| Data Validator | `/data-validator` | Compare row counts between environments to spot missing or mismatched data. |
| Plan Explorer | `/plan-explorer` (new route) | Visualise execution plans and get AI-assisted tuning recommendations. |
| Migration Runs | `/runs` (new route) | Auditable history of validate and migrate operations across environments. |
| Script Repository | `/repos` (new route) | Browse migration scripts and diff versions side by side. |

Card hover: slight lift (`translateY(-2px)`) and accent border, matching the mockup.

### 3. Environments — compact list (replaces the big card grid)

Left column of a two-column lower area (`minmax(0,3fr) / minmax(0,2fr)`; stacks on mobile).
One row per environment inside a single panel card:

- Left: environment name (bold), then a muted meta line: current version (monospace font),
  `server/database`, pending count.
- Right: status pill — `Up to date` (green) / `Pending` (amber) / `Drift detected` (red).
- Row click navigates to `/environments/:id` (existing route).
- Keep the existing env type colour conventions if shown (`--env-dev`, `--env-test`,
  `--env-staging`, `--env-prod` variables already exist in styles.css).

### 4. Guides panel

Right column, panel card titled "Guides". Simple list rows with a book icon. Initial topics:

1. Getting started with migrations
2. Resolving schema drift
3. Reading a query plan
4. Safe deploys to production
5. Writing versioned vs repeatable scripts

Implementation:

- Store guides as markdown files in `apps/web/src/guides/` (bundled via Vite `?raw` imports
  or a glob), rendered with `react-markdown` on a new route `/guides/:slug`.
- A small index module exports `{ slug, title, file }` so the dashboard list and the guide
  route share one source of truth.
- Contextual links: the "Drift detected" status pill/tile should deep-link to
  `/guides/resolving-schema-drift`.
- Existing material in `docs/architecture.md` and `docs/data-model.md` can seed content;
  guides should be written for end users, not contributors.

### 5. Recent activity panel

Right column, below Guides. Last 3–5 runs from `GET /runs`: action + environment (bold),
muted meta line (detail + relative time), status pill on the right. "View all" links to
`/runs`. Reuse the `Run` type from `apps/web/src/types.ts` and patterns from
`apps/web/src/pages/RunsPage.tsx`.

## Routing and navigation changes (`apps/web/src/App.tsx`)

- Add routes: `/plan-explorer` → `PlanExplorerPage`, `/runs` → `RunsPage`,
  `/repos` → `RepoPage`, `/guides/:slug` → new `GuidePage`.
- Add nav links for Plan Explorer and Runs to the top bar. Script Repository and Guides can
  be dashboard-only entry points to keep the nav short (5 links max).

## Code-level fixes (do these regardless of visual changes)

All in `apps/web/src/pages/DashboardPage.tsx` unless noted:

1. **Resilient status fetching.** The current `Promise.all` over
   `GET /environments/:id/status` rejects entirely if one environment is unreachable,
   leaving every card stuck on "Loading status...". Switch to `Promise.allSettled`; a failed
   environment shows a muted "Status unavailable" pill instead of blocking the rest.
2. **Empty state.** Zero environments currently renders a blank grid. Show a friendly empty
   card: "No environments yet — add one to get started", linking to wherever environments
   are configured.
3. **Loading state.** Show skeleton tiles/rows during the initial fetch instead of an empty
   page.
4. **Refresh.** Add a manual refresh button in the page header and optional 60-second
   polling (clear the interval on unmount). Show "Refreshed N min ago".
5. **Error handling.** Keep the error banner, but make it dismissible and don't let it
   replace already-loaded content.

## Styling notes

- Reuse the existing design language in `apps/web/src/styles.css`: Space Grotesk font,
  `--accent: #2d7ff9`, `--border: #d4e2ff`, 16px-radius white cards with the soft shadow
  `0 10px 30px rgba(15, 23, 42, 0.06)`, pill badges (`.badge.ok/.warn/.danger`).
- The mockup uses Tabler Icons via the webfont CDN. For the app, prefer
  `@tabler/icons-react` (tree-shakeable) over the webfont. Icon suggestions:
  git-compare (Schema Compare), checklist (Data Validator), binary-tree (Plan Explorer),
  history (Runs), folder-code (Script Repository), book (Guides), alert-triangle (drift),
  refresh.
- Responsive: grids collapse to one column under ~880px, consistent with the existing
  `@media (max-width: 800px)` rules.

## Out of scope

- No backend/API changes — everything above uses existing endpoints
  (`/environments`, `/environments/:id/status`, `/runs`).
- No auth changes (`apps/web/src/auth.tsx` untouched).
- No changes to the tool pages themselves beyond adding routes/nav.

## Acceptance criteria

- [ ] Dashboard shows summary strip, tools grid, compact environment list, guides panel,
      and recent activity, matching `mockup.html`.
- [ ] All five tools are reachable from the dashboard; `/plan-explorer`, `/runs`, and
      `/repos` routes work and Plan Explorer + Runs appear in the top nav.
- [ ] One unreachable environment degrades gracefully (other statuses still load).
- [ ] Empty, loading, and error states all render sensibly.
- [ ] At least the five listed guides exist as markdown and render at `/guides/:slug`.
- [ ] Layout works at mobile widths (single column) and desktop.
