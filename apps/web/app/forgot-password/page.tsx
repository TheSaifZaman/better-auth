"use client";

import { useState } from "react";
import Link from "next/link";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { authClient } from "../../lib/auth-client";
import { AuthShell, Banner, FormHeader, SubmitButton, TextField } from "../../components/ui";
import { emailSchema } from "../../lib/validation";

const schema = z.object({ email: emailSchema });
type Values = z.infer<typeof schema>;

export default function ForgotPasswordPage() {
    const [sent, setSent] = useState(false);
    const [serverError, setServerError] = useState("");

    const {
        register,
        handleSubmit,
        formState: { errors, isSubmitting },
    } = useForm<Values>({ resolver: zodResolver(schema), mode: "onTouched", defaultValues: { email: "" } });

    const onSubmit = handleSubmit(async ({ email }) => {
        setServerError("");
        const { error } = await authClient.requestPasswordReset({
            email,
            redirectTo: `${window.location.origin}/reset-password`,
        });
        // Always show success — don't reveal whether an account exists.
        if (error && error.status === 429) {
            setServerError("Too many requests. Please wait a moment and try again.");
            return;
        }
        setSent(true);
    });

    return (
        <AuthShell
            footer={
                <Link href="/sign-in" className="font-medium text-indigo-600 hover:text-indigo-700">
                    Back to sign in
                </Link>
            }
        >
            {sent ? (
                <div className="space-y-4">
                    <FormHeader title="Check your inbox" subtitle="If that email exists, a reset link is on its way." />
                    <Banner variant="success">
                        Open Mailpit at{" "}
                        <a href="http://localhost:8025" target="_blank" rel="noreferrer" className="font-semibold underline">
                            localhost:8025
                        </a>{" "}
                        to find the reset link.
                    </Banner>
                </div>
            ) : (
                <form onSubmit={onSubmit} className="space-y-4" noValidate>
                    <FormHeader title="Reset your password" subtitle="We'll email you a link to set a new one." />
                    {serverError && <Banner variant="error">{serverError}</Banner>}
                    <TextField
                        id="forgot-email"
                        label="Email"
                        type="email"
                        inputMode="email"
                        autoComplete="email"
                        placeholder="you@example.com"
                        registration={register("email")}
                        error={errors.email?.message}
                    />
                    <SubmitButton loading={isSubmitting}>Send reset link</SubmitButton>
                </form>
            )}
        </AuthShell>
    );
}
