"use client";

import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useForm } from "react-hook-form";
import { zodResolver } from "@hookform/resolvers/zod";
import { authClient } from "../lib/auth-client";
import {
    signInSchema,
    signUpSchema,
    type SignInValues,
    type SignUpValues,
} from "../lib/validation";
import {
    Banner,
    Checkbox,
    FormHeader,
    PasswordField,
    PasswordStrength,
    SecondaryButton,
    SubmitButton,
    TextField,
    mono,
} from "./ui";

export { AuthShell } from "./ui";

type AuthError = { status?: number; message?: string; code?: string } | null | undefined;

function describeError(error: AuthError, fallback: string): string {
    if (!error) return fallback;
    if (error.status === 429) return "Too many attempts. Please wait a moment and try again.";
    return error.message || fallback;
}

/** Countdown used for the rate-limit lockout UX. */
function useCountdown(): [number, (seconds: number) => void] {
    const [remaining, setRemaining] = useState(0);
    useEffect(() => {
        if (remaining <= 0) return;
        const t = setInterval(() => setRemaining((r) => Math.max(0, r - 1)), 1000);
        return () => clearInterval(t);
    }, [remaining]);
    return [remaining, setRemaining];
}

// --------------------------------------------------------------------------
// Sign in
// --------------------------------------------------------------------------

export function LoginForm() {
    const router = useRouter();
    const [step, setStep] = useState<"credentials" | "totp">("credentials");
    const [serverError, setServerError] = useState("");
    const [needsVerify, setNeedsVerify] = useState(false);
    const [resent, setResent] = useState("");
    const [lock, setLock] = useCountdown();

    const {
        register,
        handleSubmit,
        getValues,
        formState: { errors, isSubmitting },
    } = useForm<SignInValues>({
        resolver: zodResolver(signInSchema),
        mode: "onTouched",
        defaultValues: { email: "", password: "", rememberMe: true },
    });

    const onSubmit = handleSubmit(async (values) => {
        setServerError("");
        setNeedsVerify(false);
        setResent("");
        const { data, error } = await authClient.signIn.email({
            email: values.email,
            password: values.password,
            rememberMe: values.rememberMe,
        });
        if (error) {
            if (error.status === 429) {
                setLock(60);
                return;
            }
            if (error.code === "EMAIL_NOT_VERIFIED" || /verif/i.test(error.message ?? "")) {
                setNeedsVerify(true);
                return;
            }
            setServerError(describeError(error, "Could not sign you in. Check your details."));
            return;
        }
        if ((data as { twoFactorRedirect?: boolean } | null)?.twoFactorRedirect) {
            setStep("totp");
            return;
        }
        router.push("/");
    });

    const resendVerification = async () => {
        setResent("");
        await authClient.sendVerificationEmail({
            email: getValues("email"),
            callbackURL: `${window.location.origin}/`,
        });
        setResent("Verification email sent. Check Mailpit at localhost:8025.");
    };

    if (step === "totp") {
        return <TotpStep onDone={() => router.push("/")} onBack={() => setStep("credentials")} />;
    }

    return (
        <form onSubmit={onSubmit} className="space-y-4" noValidate>
            <FormHeader title="Welcome back" subtitle="Sign in to continue to your account." />

            {serverError && <Banner variant="error">{serverError}</Banner>}
            {lock > 0 && (
                <Banner variant="error">
                    Locked for {lock}s after too many attempts.
                </Banner>
            )}
            {needsVerify && (
                <Banner variant="info">
                    Your email isn't verified yet.{" "}
                    <button type="button" onClick={resendVerification} className="font-semibold underline">
                        Resend verification
                    </button>
                </Banner>
            )}
            {resent && <Banner variant="success">{resent}</Banner>}

            <TextField
                id="signin-email"
                label="Email"
                type="email"
                inputMode="email"
                autoComplete="email"
                placeholder="you@example.com"
                registration={register("email")}
                error={errors.email?.message}
            />
            <PasswordField
                id="signin-password"
                label="Password"
                autoComplete="current-password"
                placeholder="••••••••"
                registration={register("password")}
                error={errors.password?.message}
            />
            <div className="flex items-center justify-between pt-1">
                <Checkbox id="rememberMe" label="Remember me" registration={register("rememberMe")} />
                <Link href="/forgot-password" className="text-sm font-medium text-indigo-600 hover:text-indigo-700">
                    Forgot password?
                </Link>
            </div>
            <SubmitButton loading={isSubmitting} disabled={lock > 0}>
                Sign in
            </SubmitButton>
        </form>
    );
}

// --------------------------------------------------------------------------
// Two-factor step (shown after a correct password when 2FA is enabled)
// --------------------------------------------------------------------------

function TotpStep({ onDone, onBack }: { onDone: () => void; onBack: () => void }) {
    const [useBackup, setUseBackup] = useState(false);
    const [serverError, setServerError] = useState("");
    const [trustDevice, setTrustDevice] = useState(true);

    // The input accepts two very different formats, so validate per-mode
    // rather than with a fixed schema: 6 digits for TOTP, an alphanumeric
    // backup code otherwise.
    const {
        register,
        handleSubmit,
        formState: { errors, isSubmitting },
    } = useForm<{ code: string }>({
        mode: "onTouched",
        defaultValues: { code: "" },
    });

    const onSubmit = handleSubmit(async ({ code }) => {
        setServerError("");
        const value = code.trim();
        const action = useBackup
            ? authClient.twoFactor.verifyBackupCode({ code: value })
            : authClient.twoFactor.verifyTotp({ code: value.replace(/\s/g, ""), trustDevice });
        const { error } = await action;
        if (error) {
            setServerError(describeError(error, "That code didn't work. Try again."));
            return;
        }
        onDone();
    });

    return (
        <form onSubmit={onSubmit} className="space-y-4" noValidate>
            <FormHeader
                title="Two-factor verification"
                subtitle={
                    useBackup
                        ? "Enter one of your saved backup codes."
                        : "Enter the 6-digit code from your authenticator app."
                }
            />
            {serverError && <Banner variant="error">{serverError}</Banner>}

            <TextField
                id="totp-code"
                label={useBackup ? "Backup code" : "Authentication code"}
                inputMode={useBackup ? "text" : "numeric"}
                autoComplete="one-time-code"
                placeholder={useBackup ? "xxxxx-xxxxx" : "123456"}
                registration={register("code", {
                    required: "Enter a code",
                    validate: (v) => {
                        const t = v.trim();
                        if (useBackup) return t.length >= 6 || "Enter a valid backup code";
                        return /^\d{6}$/.test(t.replace(/\s/g, "")) || "Enter the 6-digit code";
                    },
                })}
                error={errors.code?.message}
            />

            {!useBackup && (
                <label className="flex cursor-pointer items-center gap-2 text-sm text-slate-600">
                    <input
                        type="checkbox"
                        checked={trustDevice}
                        onChange={(e) => setTrustDevice(e.target.checked)}
                        className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-2 focus:ring-indigo-500 focus:ring-offset-1"
                    />
                    Trust this device for 60 days
                </label>
            )}

            <SubmitButton loading={isSubmitting}>Verify</SubmitButton>

            <div className="flex items-center justify-between">
                <button
                    type="button"
                    onClick={() => setUseBackup((b) => !b)}
                    className={`${mono} text-[11px] uppercase tracking-wider text-slate-400 hover:text-indigo-600`}
                >
                    {useBackup ? "Use authenticator" : "Use a backup code"}
                </button>
                <button
                    type="button"
                    onClick={onBack}
                    className={`${mono} text-[11px] uppercase tracking-wider text-slate-400 hover:text-indigo-600`}
                >
                    ← Back
                </button>
            </div>
        </form>
    );
}

// --------------------------------------------------------------------------
// Sign up
// --------------------------------------------------------------------------

export function Signupform() {
    const [serverError, setServerError] = useState("");
    const [done, setDone] = useState<null | string>(null);

    const {
        register,
        handleSubmit,
        watch,
        formState: { errors, isSubmitting },
    } = useForm<SignUpValues>({
        resolver: zodResolver(signUpSchema),
        mode: "onTouched",
        defaultValues: { name: "", email: "", password: "", confirmPassword: "" },
    });

    const passwordValue = watch("password");

    const onSubmit = handleSubmit(async (values) => {
        setServerError("");
        const { error } = await authClient.signUp.email({
            name: values.name,
            email: values.email,
            password: values.password,
            callbackURL: `${window.location.origin}/`,
        });
        if (error) {
            if (error.status === 429) {
                setServerError("Too many attempts. Please wait a moment and try again.");
                return;
            }
            setServerError(describeError(error, "Could not create your account."));
            return;
        }
        setDone(values.email);
    });

    if (done) {
        return (
            <div className="space-y-4">
                <FormHeader
                    title="Check your inbox"
                    subtitle={`We sent a verification link to ${done}.`}
                />
                <Banner variant="success">
                    Open Mailpit at{" "}
                    <a href="http://localhost:8025" target="_blank" rel="noreferrer" className="font-semibold underline">
                        localhost:8025
                    </a>{" "}
                    and click the link to activate your account.
                </Banner>
                <p className="text-sm text-slate-500">
                    Verify your email, then head to the{" "}
                    <Link href="/sign-in" className="font-medium text-indigo-600 hover:text-indigo-700">
                        sign-in page
                    </Link>
                    .
                </p>
            </div>
        );
    }

    return (
        <form onSubmit={onSubmit} className="space-y-4" noValidate>
            <FormHeader title="Create your account" subtitle="Set up secure access in a few seconds." />
            {serverError && <Banner variant="error">{serverError}</Banner>}

            <TextField
                id="signup-name"
                label="Name"
                autoComplete="name"
                placeholder="Ada Lovelace"
                registration={register("name")}
                error={errors.name?.message}
            />
            <TextField
                id="signup-email"
                label="Email"
                type="email"
                inputMode="email"
                autoComplete="email"
                placeholder="you@example.com"
                registration={register("email")}
                error={errors.email?.message}
            />
            <PasswordField
                id="signup-password"
                label="Password"
                autoComplete="new-password"
                placeholder="At least 8 characters"
                registration={register("password")}
                error={errors.password?.message}
            >
                <PasswordStrength value={passwordValue} />
            </PasswordField>
            <PasswordField
                id="signup-confirm"
                label="Confirm password"
                autoComplete="new-password"
                placeholder="Re-enter your password"
                registration={register("confirmPassword")}
                error={errors.confirmPassword?.message}
            />
            <SubmitButton loading={isSubmitting}>Create account</SubmitButton>
        </form>
    );
}

export { SecondaryButton };
