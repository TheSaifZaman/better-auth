"use client";

import { useState } from "react";
import QRCode from "qrcode";
import { authClient } from "../lib/auth-client";
import { Banner, SecondaryButton, mono } from "./ui";

type Stage = "idle" | "password" | "activate";

export function TwoFactorSetup({
    enabled,
    onChange,
}: {
    enabled: boolean;
    onChange: () => void;
}) {
    const [stage, setStage] = useState<Stage>("idle");
    const [password, setPassword] = useState("");
    const [code, setCode] = useState("");
    const [qr, setQr] = useState("");
    const [backupCodes, setBackupCodes] = useState<string[]>([]);
    const [error, setError] = useState("");
    const [busy, setBusy] = useState(false);

    const reset = () => {
        setStage("idle");
        setPassword("");
        setCode("");
        setQr("");
        setBackupCodes([]);
        setError("");
    };

    const beginEnable = async () => {
        setError("");
        setBusy(true);
        const { data, error } = await authClient.twoFactor.enable({ password });
        setBusy(false);
        if (error || !data) {
            setError(error?.message || "Couldn't start setup. Check your password.");
            return;
        }
        setQr(await QRCode.toDataURL(data.totpURI, { margin: 1, width: 200 }));
        setBackupCodes(data.backupCodes);
        setStage("activate");
    };

    const confirmEnable = async () => {
        setError("");
        setBusy(true);
        const { error } = await authClient.twoFactor.verifyTotp({ code });
        setBusy(false);
        if (error) {
            setError(error.message || "That code didn't work. Try again.");
            return;
        }
        reset();
        onChange();
    };

    const disable = async () => {
        setError("");
        setBusy(true);
        const { error } = await authClient.twoFactor.disable({ password });
        setBusy(false);
        if (error) {
            setError(error.message || "Couldn't disable. Check your password.");
            return;
        }
        reset();
        onChange();
    };

    return (
        <div className="rounded-xl border border-slate-200 bg-white p-5 text-left">
            <div className="flex items-center justify-between">
                <div>
                    <p className={`${mono} text-[11px] uppercase tracking-[0.2em] text-slate-500`}>
                        Two-factor auth
                    </p>
                    <p className="mt-0.5 text-sm text-slate-600">
                        {enabled ? "Active on your account" : "Add a second layer of security"}
                    </p>
                </div>
                <span
                    className={`${mono} rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider ${
                        enabled ? "bg-emerald-50 text-emerald-600" : "bg-slate-100 text-slate-500"
                    }`}
                >
                    {enabled ? "On" : "Off"}
                </span>
            </div>

            {error && (
                <div className="mt-3">
                    <Banner variant="error">{error}</Banner>
                </div>
            )}

            {/* Enabled → allow disabling */}
            {enabled && stage === "idle" && (
                <div className="mt-4">
                    <SecondaryButton onClick={() => setStage("password")}>Disable two-factor</SecondaryButton>
                </div>
            )}
            {enabled && stage === "password" && (
                <div className="mt-4 space-y-3">
                    <PasswordInput value={password} onChange={setPassword} placeholder="Confirm your password" />
                    <div className="grid grid-cols-2 gap-2">
                        <SecondaryButton onClick={reset}>Cancel</SecondaryButton>
                        <button
                            type="button"
                            onClick={disable}
                            disabled={busy || !password}
                            className="inline-flex items-center justify-center rounded-lg bg-red-600 px-4 py-2.5 text-sm font-semibold text-white transition-all hover:bg-red-700 disabled:opacity-60"
                        >
                            {busy ? "Working…" : "Disable"}
                        </button>
                    </div>
                </div>
            )}

            {/* Disabled → enable flow */}
            {!enabled && stage === "idle" && (
                <div className="mt-4">
                    <SecondaryButton onClick={() => setStage("password")}>Enable two-factor</SecondaryButton>
                </div>
            )}
            {!enabled && stage === "password" && (
                <div className="mt-4 space-y-3">
                    <p className="text-sm text-slate-500">Confirm your password to generate a secret.</p>
                    <PasswordInput value={password} onChange={setPassword} placeholder="Your password" />
                    <div className="grid grid-cols-2 gap-2">
                        <SecondaryButton onClick={reset}>Cancel</SecondaryButton>
                        <button
                            type="button"
                            onClick={beginEnable}
                            disabled={busy || !password}
                            className="inline-flex items-center justify-center rounded-lg bg-indigo-600 px-4 py-2.5 text-sm font-semibold text-white transition-all hover:bg-indigo-700 disabled:opacity-60"
                        >
                            {busy ? "Working…" : "Continue"}
                        </button>
                    </div>
                </div>
            )}
            {!enabled && stage === "activate" && (
                <div className="mt-4 space-y-4">
                    <div className="flex flex-col items-center gap-3">
                        <p className="text-sm text-slate-500">Scan with your authenticator app, then enter a code.</p>
                        {qr && (
                            // eslint-disable-next-line @next/next/no-img-element
                            <img src={qr} alt="Two-factor QR code" className="rounded-lg border border-slate-200" />
                        )}
                    </div>

                    <div>
                        <p className={`${mono} mb-1.5 text-[11px] uppercase tracking-wider text-slate-500`}>
                            Backup codes — save these
                        </p>
                        <ul className={`${mono} grid grid-cols-2 gap-1 rounded-lg bg-slate-50 p-3 text-xs text-slate-700`}>
                            {backupCodes.map((c) => (
                                <li key={c}>{c}</li>
                            ))}
                        </ul>
                    </div>

                    <div className="space-y-2">
                        <input
                            value={code}
                            onChange={(e) => setCode(e.target.value)}
                            inputMode="numeric"
                            placeholder="123456"
                            className="w-full rounded-lg border border-slate-200 bg-slate-50/60 px-3.5 py-2.5 text-sm focus:border-indigo-500 focus:bg-white focus:outline-none focus:ring-4 focus:ring-indigo-500/10"
                        />
                        <div className="grid grid-cols-2 gap-2">
                            <SecondaryButton onClick={reset}>Cancel</SecondaryButton>
                            <button
                                type="button"
                                onClick={confirmEnable}
                                disabled={busy || code.length < 6}
                                className="inline-flex items-center justify-center rounded-lg bg-indigo-600 px-4 py-2.5 text-sm font-semibold text-white transition-all hover:bg-indigo-700 disabled:opacity-60"
                            >
                                {busy ? "Verifying…" : "Activate"}
                            </button>
                        </div>
                    </div>
                </div>
            )}
        </div>
    );
}

function PasswordInput({
    value,
    onChange,
    placeholder,
}: {
    value: string;
    onChange: (v: string) => void;
    placeholder: string;
}) {
    return (
        <input
            type="password"
            value={value}
            onChange={(e) => onChange(e.target.value)}
            placeholder={placeholder}
            autoComplete="current-password"
            className="w-full rounded-lg border border-slate-200 bg-slate-50/60 px-3.5 py-2.5 text-sm focus:border-indigo-500 focus:bg-white focus:outline-none focus:ring-4 focus:ring-indigo-500/10"
        />
    );
}
