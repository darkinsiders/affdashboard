# Live Dashboard

This folder contains the current live affiliate dashboard system.

## What is here

- `app/index.html`: the live dashboard UI
- `smartlinks/`: smartlink HTML files used in production
- `postback-function/`: Firebase postback ingestion
- `cloudflare-worker/`: Cloudflare worker used by the live system
- `docs/postback-urls.txt`: postback URL notes/templates

## Source of truth

This is the current production-style dashboard and its supporting infrastructure.

## Backend

- Firebase / Firestore
- Firebase Functions
- Cloudflare Worker

## Notes

If functionality already exists today, it should be assumed to live here first.
