"use client";

import { useState } from "react";
import Link from "next/link";
import type { UseFormRegisterReturn } from "react-hook-form";
import { passwordRules, passwordStrength } from "../lib/validation";

export const mono = "font-[family-name:var(--font-geist-mono)]";

export function AuthShell({
    children,
    footer,
}: {
    children: React.ReactNode;
    footer: React.ReactNode;
}) {
    return (
        <main className="grid min-h-screen place-items-center bg-slate-50 px-4 py-12">
            <div className="w-full max-w-md">
                <Link
                    href="/"
                    className={`${mono} inline-flex items-center gap-1 text-[11px] uppercase tracking-[0.25em] text-slate-400 transition-colors hover:text-indigo-600`}
                >
                    <span aria-hidden>←</span> better-auth
                </Link>
                <div className="mt-3 rounded-2xl border border-slate-200 bg-white p-8 shadow-xl shadow-slate-900/5">
                    {children}
                </div>
                <p className="mt-5 text-center text-sm text-slate-500">{footer}</p>
            </div>
        </main>
    );
}

export function FormHeader({ title, subtitle }: { title: string; subtitle: string }) {
    return (
        <div className="mb-6">
            <p className={`${mono} text-[11px] uppercase tracking-[0.25em] text-indigo-600`}>
                Secure access<span className="caret">▍</span>
            </p>
            <h1 className="mt-2 text-xl font-semibold tracking-tight text-slate-900">{title}</h1>
            <p className="mt-1 text-sm text-slate-500">{subtitle}</p>
        </div>
    );
}

function Label({ htmlFor, children }: { htmlFor: string; children: React.ReactNode }) {
    return (
        <label
            htmlFor={htmlFor}
            className={`${mono} block text-[11px] font-medium uppercase tracking-[0.15em] text-slate-500`}
        >
            {children}
        </label>
    );
}

const inputBase =
    "w-full rounded-lg border bg-slate-50/60 px-3.5 py-2.5 text-sm text-slate-900 placeholder:text-slate-400 transition-colors duration-150 focus:bg-white focus:outline-none focus:ring-4";

function FieldError({ id, message }: { id: string; message?: string }) {
    if (!message) return null;
    return (
        <p id={id} className={`${mono} text-xs text-red-600`} role="alert">
            {message}
        </p>
    );
}

export function TextField({
    id,
    label,
    type = "text",
    placeholder,
    autoComplete,
    registration,
    error,
    inputMode,
}: {
    id: string;
    label: string;
    type?: string;
    placeholder?: string;
    autoComplete?: string;
    registration: UseFormRegisterReturn;
    error?: string;
    inputMode?: "text" | "numeric" | "email";
}) {
    return (
        <div className="space-y-1.5">
            <Label htmlFor={id}>{label}</Label>
            <input
                id={id}
                type={type}
                placeholder={placeholder}
                autoComplete={autoComplete}
                inputMode={inputMode}
                aria-invalid={!!error}
                aria-describedby={error ? `${id}-error` : undefined}
                {...registration}
                className={`${inputBase} ${
                    error
                        ? "border-red-300 focus:border-red-500 focus:ring-red-500/10"
                        : "border-slate-200 focus:border-indigo-500 focus:ring-indigo-500/10"
                }`}
            />
            <FieldError id={`${id}-error`} message={error} />
        </div>
    );
}

export function PasswordField({
    id,
    label,
    placeholder,
    autoComplete,
    registration,
    error,
    children,
}: {
    id: string;
    label: string;
    placeholder?: string;
    autoComplete?: string;
    registration: UseFormRegisterReturn;
    error?: string;
    children?: React.ReactNode;
}) {
    const [show, setShow] = useState(false);
    return (
        <div className="space-y-1.5">
            <Label htmlFor={id}>{label}</Label>
            <div className="relative">
                <input
                    id={id}
                    type={show ? "text" : "password"}
                    placeholder={placeholder}
                    autoComplete={autoComplete}
                    aria-invalid={!!error}
                    aria-describedby={error ? `${id}-error` : undefined}
                    {...registration}
                    className={`${inputBase} pr-16 ${
                        error
                            ? "border-red-300 focus:border-red-500 focus:ring-red-500/10"
                            : "border-slate-200 focus:border-indigo-500 focus:ring-indigo-500/10"
                    }`}
                />
                <button
                    type="button"
                    onClick={() => setShow((s) => !s)}
                    aria-pressed={show}
                    className={`${mono} absolute inset-y-0 right-0 flex items-center px-3 text-[10px] uppercase tracking-widest text-slate-400 transition-colors hover:text-indigo-600`}
                >
                    {show ? "Hide" : "Show"}
                </button>
            </div>
            <FieldError id={`${id}-error`} message={error} />
            {children}
        </div>
    );
}

export function PasswordStrength({ value }: { value: string }) {
    const { score, label } = passwordStrength(value);
    return (
        <div className="mt-2 space-y-2">
            <div className="flex items-center gap-2">
                <div className="flex flex-1 gap-1">
                    {[1, 2, 3, 4].map((seg) => (
                        <span
                            key={seg}
                            className={`h-1 flex-1 rounded-full transition-colors ${
                                seg <= score ? passwordStrength(value).color : "bg-slate-200"
                            }`}
                        />
                    ))}
                </div>
                {label && (
                    <span className={`${mono} w-14 text-right text-[10px] uppercase tracking-wider text-slate-500`}>
                        {label}
                    </span>
                )}
            </div>
            <ul className="grid grid-cols-2 gap-x-3 gap-y-1">
                {passwordRules.map((rule) => {
                    const ok = rule.test(value);
                    return (
                        <li
                            key={rule.label}
                            className={`${mono} flex items-center gap-1.5 text-[11px] ${
                                ok ? "text-emerald-600" : "text-slate-400"
                            }`}
                        >
                            <span aria-hidden>{ok ? "✓" : "○"}</span>
                            {rule.label}
                        </li>
                    );
                })}
            </ul>
        </div>
    );
}

export function Checkbox({
    id,
    label,
    registration,
}: {
    id: string;
    label: string;
    registration: UseFormRegisterReturn;
}) {
    return (
        <label htmlFor={id} className="flex cursor-pointer items-center gap-2 text-sm text-slate-600">
            <input
                id={id}
                type="checkbox"
                {...registration}
                className="h-4 w-4 rounded border-slate-300 text-indigo-600 focus:ring-2 focus:ring-indigo-500 focus:ring-offset-1"
            />
            {label}
        </label>
    );
}

export function SubmitButton({
    loading,
    disabled,
    children,
}: {
    loading: boolean;
    disabled?: boolean;
    children: React.ReactNode;
}) {
    return (
        <button
            type="submit"
            disabled={loading || disabled}
            className="mt-1 flex w-full items-center justify-center rounded-lg bg-indigo-600 px-4 py-2.5 text-sm font-semibold text-white shadow-sm transition-all duration-150 hover:bg-indigo-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 focus-visible:ring-offset-2 active:scale-[0.99] disabled:cursor-not-allowed disabled:opacity-60"
        >
            {loading ? "Working…" : children}
        </button>
    );
}

export function SecondaryButton({
    onClick,
    children,
    type = "button",
}: {
    onClick?: () => void;
    children: React.ReactNode;
    type?: "button" | "submit";
}) {
    return (
        <button
            type={type}
            onClick={onClick}
            className="inline-flex w-full items-center justify-center rounded-lg border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 transition-colors hover:bg-slate-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 focus-visible:ring-offset-2"
        >
            {children}
        </button>
    );
}

type BannerVariant = "error" | "success" | "info";

export function Banner({ variant, children }: { variant: BannerVariant; children: React.ReactNode }) {
    if (!children) return null;
    const styles: Record<BannerVariant, string> = {
        error: "border-red-200 bg-red-50 text-red-700",
        success: "border-emerald-200 bg-emerald-50 text-emerald-700",
        info: "border-indigo-200 bg-indigo-50 text-indigo-700",
    };
    const glyph: Record<BannerVariant, string> = { error: "!", success: "✓", info: "i" };
    return (
        <div
            role={variant === "error" ? "alert" : "status"}
            className={`${mono} flex items-start gap-2 rounded-lg border px-3 py-2.5 text-xs ${styles[variant]}`}
        >
            <span aria-hidden className="mt-px font-bold">
                {glyph[variant]}
            </span>
            <span className="leading-relaxed">{children}</span>
        </div>
    );
}
