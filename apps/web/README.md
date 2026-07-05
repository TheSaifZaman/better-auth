# web

Next.js frontend for the [better-auth monorepo](../../README.md) — the authentication UI.

## What's here

- **Sign up / sign in / password reset** pages backed by the better-auth client (`lib/auth-client.ts`).
- Talks to the NestJS API (`apps/backend`, mounted at `/api/auth`) via a Next.js rewrite/proxy so cookies and the real client IP are forwarded.

## Develop

```sh
pnpm --filter web dev     # http://localhost:3001
```

Make sure the backend is running (`pnpm --filter backend dev`, on `:3000`) and that `TRUSTED_ORIGINS` in the backend env includes this app's origin.

## Key files

- `app/sign-in`, `app/sign-up`, `app/forgot-password`, `app/reset-password` — auth pages
- `components/auth-form.tsx` — shared form
- `lib/auth-client.ts` — better-auth client + API base URL
- `next.config.js` — proxy rewrite to the backend
