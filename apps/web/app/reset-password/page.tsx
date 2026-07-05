"use client";

import { Suspense, useState } from "react";
import Link from "next/link";
import { useRouter, useSearchParams } from "next/navigation";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { z } from "zod";
import { authClient } from "../../lib/auth-client";
import {
    AuthShell,
    Banner,
    FormHeader,
    PasswordField,
    PasswordStrength,
    SubmitButton,
} from "../../components/ui";
import { passwordSchema } from "../../lib/validation";

const schema = z
    .object({ password: passwordSchema, confirmPassword: z.string().min(1, "Confirm your password") })
    .refine((d) => d.password === d.confirmPassword, {
        message: "Passwords don't match",
        path: ["confirmPassword"],
    });
type Values = z.infer<typeof schema>;

function ResetForm() {
    const router = useRouter();
    const params = useSearchParams();
    const token = params.get("token") ?? "";
    // better-auth redirects here with ?error=... (any casing) when the link is bad.
    const invalidLink = !token || params.has("error");
    const [serverError, setServerError] = useState("");
    const [done, setDone] = useState(false);

    const {
        register,
        handleSubmit,
        watch,
        formState: { errors, isSubmitting },
    } = useForm<Values>({
        resolver: zodResolver(schema),
        mode: "onTouched",
        defaultValues: { password: "", confirmPassword: "" },
    });

    const onSubmit = handleSubmit(async ({ password }) => {
        setServerError("");
        const { error } = await authClient.resetPassword({ newPassword: password, token });
        if (error) {
            setServerError(error.message || "Couldn't reset your password. The link may have expired.");
            return;
        }
        setDone(true);
        setTimeout(() => router.push("/sign-in"), 1200);
    });

    if (invalidLink) {
        return (
            <>
                <FormHeader title="Link expired" subtitle="This reset link is invalid or has expired." />
                <Banner variant="error">Request a new link to continue.</Banner>
                <div className="mt-4">
                    <Link href="/forgot-password" className="font-medium text-indigo-600 hover:text-indigo-700">
                        Send a new reset link
                    </Link>
                </div>
            </>
        );
    }

    if (done) {
        return (
            <>
                <FormHeader title="Password updated" subtitle="Redirecting you to sign in…" />
                <Banner variant="success">You can now sign in with your new password.</Banner>
            </>
        );
    }

    return (
        <form onSubmit={onSubmit} className="space-y-4" noValidate>
            <FormHeader title="Set a new password" subtitle="Choose a strong password you don't use elsewhere." />
            {serverError && <Banner variant="error">{serverError}</Banner>}
            <PasswordField
                id="reset-password"
                label="New password"
                autoComplete="new-password"
                placeholder="At least 8 characters"
                registration={register("password")}
                error={errors.password?.message}
            >
                <PasswordStrength value={watch("password")} />
            </PasswordField>
            <PasswordField
                id="reset-confirm"
                label="Confirm password"
                autoComplete="new-password"
                placeholder="Re-enter your password"
                registration={register("confirmPassword")}
                error={errors.confirmPassword?.message}
            />
            <SubmitButton loading={isSubmitting}>Update password</SubmitButton>
        </form>
    );
}

export default function ResetPasswordPage() {
    return (
        <AuthShell
            footer={
                <Link href="/sign-in" className="font-medium text-indigo-600 hover:text-indigo-700">
                    Back to sign in
                </Link>
            }
        >
            <Suspense fallback={<p className="text-sm text-slate-500">Loading…</p>}>
                <ResetForm />
            </Suspense>
        </AuthShell>
    );
}
