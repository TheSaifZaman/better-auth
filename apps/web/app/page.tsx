"use client";

import Link from "next/link";
import { authClient } from "../lib/auth-client";
import { TwoFactorSetup } from "../components/two-factor-setup";

const mono = "font-[family-name:var(--font-geist-mono)]";

export default function Home() {
    const { data: session, isPending: isLoading, refetch } = authClient.useSession();

    if (isLoading) {
        return (
            <main className="grid min-h-screen place-items-center bg-slate-50">
                <p className={`${mono} text-sm uppercase tracking-[0.25em] text-slate-400`}>
                    Loading<span className="caret">▍</span>
                </p>
            </main>
        );
    }

    if (session) {
        const user = session.user as typeof session.user & {
            twoFactorEnabled?: boolean | null;
        };
        return (
            <main className="grid min-h-screen place-items-center bg-slate-50 px-4 py-12">
                <div className="w-full max-w-md">
                    <div className="rounded-2xl border border-slate-200 bg-white p-8 shadow-xl shadow-slate-900/5">
                        <p className={`${mono} text-[11px] uppercase tracking-[0.25em] text-emerald-600`}>
                            Session active<span className="caret">▍</span>
                        </p>
                        <h1 className="mt-3 text-2xl font-semibold tracking-tight text-slate-900">
                            Welcome, {user.name}
                        </h1>
                        <div className="mt-2 flex items-center gap-2 text-sm text-slate-500">
                            <span>{user.email}</span>
                            <span
                                className={`${mono} rounded-full px-2 py-0.5 text-[10px] uppercase tracking-wider ${
                                    user.emailVerified
                                        ? "bg-emerald-50 text-emerald-600"
                                        : "bg-amber-50 text-amber-600"
                                }`}
                            >
                                {user.emailVerified ? "Verified" : "Unverified"}
                            </span>
                        </div>

                        <div className="mt-6">
                            <TwoFactorSetup
                                enabled={!!user.twoFactorEnabled}
                                onChange={() => refetch()}
                            />
                        </div>

                        <button
                            onClick={() => authClient.signOut()}
                            className="mt-6 inline-flex w-full items-center justify-center rounded-lg border border-slate-200 bg-white px-4 py-2.5 text-sm font-semibold text-slate-700 transition-colors hover:bg-slate-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 focus-visible:ring-offset-2"
                        >
                            Sign out
                        </button>
                    </div>
                </div>
            </main>
        );
    }

    return (
        <main className="grid min-h-screen place-items-center bg-slate-50 px-4 py-12">
            <div className="w-full max-w-md text-center">
                <p className={`${mono} text-[11px] uppercase tracking-[0.3em] text-indigo-600`}>
                    Better&#8209;Auth · Next.js<span className="caret">▍</span>
                </p>
                <h1 className="mt-4 text-4xl font-semibold tracking-tight text-slate-900">Welcome</h1>
                <p className="mx-auto mt-3 max-w-sm text-base text-slate-500">
                    Sign in to your account, or create a new one to get started.
                </p>

                <div
                    className={`${mono} mx-auto mt-8 max-w-xs rounded-xl border border-slate-200 bg-white px-4 py-3 text-left text-xs text-slate-500 shadow-sm`}
                >
                    <span className="text-slate-400">$</span> auth status:{" "}
                    <span className="text-amber-600">awaiting sign&#8209;in</span>
                </div>

                <div className="mt-8 flex flex-col gap-3 sm:flex-row sm:justify-center">
                    <Link
                        href="/sign-in"
                        className="inline-flex items-center justify-center rounded-lg bg-indigo-600 px-6 py-2.5 text-sm font-semibold text-white shadow-sm transition-all duration-150 hover:bg-indigo-700 focus:outline-none focus-visible:ring-2 focus-visible:ring-indigo-500 focus-visible:ring-offset-2 active:scale-[0.99]"
                    >
                        Sign in
                    </Link>
                    <Link
                        href="/sign-up"
                        className="inline-flex items-center justify-center rounded-lg border border-slate-200 bg-white px-6 py-2.5 text-sm font-semibold text-slate-700 transition-colors hover:bg-slate-50 focus:outline-none focus-visible:ring-2 focus-visible:ring-slate-400 focus-visible:ring-offset-2"
                    >
                        Create account
                    </Link>
                </div>
            </div>
        </main>
    );
}
