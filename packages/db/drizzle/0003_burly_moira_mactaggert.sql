CREATE TABLE "audit_log" (
	"id" bigserial PRIMARY KEY NOT NULL,
	"event_type" text NOT NULL,
	"workflow_id" text,
	"run_id" text,
	"payload" jsonb NOT NULL,
	"kafka_partition" integer NOT NULL,
	"kafka_offset" bigint NOT NULL,
	"created_at" timestamp DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "workflow_run_counts" (
	"workflow_id" text PRIMARY KEY NOT NULL,
	"completed_count" integer DEFAULT 0 NOT NULL,
	"updated_at" timestamp DEFAULT now() NOT NULL
);
