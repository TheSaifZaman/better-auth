import { z } from "zod";

/**
 * Shared validation for the auth forms. These schemas are the single source
 * of truth on the client (react-hook-form) and mirror what better-auth
 * enforces on the server (min/max length, email format, breach check).
 */

export const emailSchema = z
    .string()
    .min(1, "Email is required")
    .email("Enter a valid email address");

// Individual password requirements — also drive the live checklist + meter.
export const passwordRules: { label: string; test: (v: string) => boolean }[] = [
    { label: "At least 8 characters", test: (v) => v.length >= 8 },
    { label: "An uppercase letter", test: (v) => /[A-Z]/.test(v) },
    { label: "A lowercase letter", test: (v) => /[a-z]/.test(v) },
    { label: "A number", test: (v) => /[0-9]/.test(v) },
    { label: "A symbol", test: (v) => /[^A-Za-z0-9]/.test(v) },
];

export const passwordSchema = z
    .string()
    .min(8, "Use at least 8 characters")
    .max(128, "Keep it under 128 characters")
    .regex(/[A-Z]/, "Add an uppercase letter")
    .regex(/[a-z]/, "Add a lowercase letter")
    .regex(/[0-9]/, "Add a number")
    .regex(/[^A-Za-z0-9]/, "Add a symbol");

export const signInSchema = z.object({
    email: emailSchema,
    // On sign-in we only require presence — the account may predate current rules.
    password: z.string().min(1, "Password is required"),
    rememberMe: z.boolean().optional(),
});

export const signUpSchema = z
    .object({
        name: z.string().min(2, "Enter your name").max(64, "That name is too long"),
        email: emailSchema,
        password: passwordSchema,
        confirmPassword: z.string().min(1, "Confirm your password"),
    })
    .refine((d) => d.password === d.confirmPassword, {
        message: "Passwords don't match",
        path: ["confirmPassword"],
    });

export const totpSchema = z.object({
    code: z
        .string()
        .min(6, "Enter the 6-digit code")
        .max(6, "Codes are 6 digits")
        .regex(/^\d{6}$/, "Digits only"),
});

export type SignInValues = z.infer<typeof signInSchema>;
export type SignUpValues = z.infer<typeof signUpSchema>;
export type TotpValues = z.infer<typeof totpSchema>;

export type Strength = {
    score: 0 | 1 | 2 | 3 | 4;
    label: string;
    color: string; // tailwind bg-* class for the meter fill
};

/** Rule-based strength estimate (0 empty → 4 strong). */
export function passwordStrength(value: string): Strength {
    if (!value) return { score: 0, label: "", color: "bg-slate-200" };
    const passed = passwordRules.filter((r) => r.test(value)).length;
    const lengthBonus = value.length >= 12 ? 1 : 0;
    const raw = Math.min(4, Math.max(1, passed - 1 + lengthBonus));
    const map: Record<number, Strength> = {
        1: { score: 1, label: "Weak", color: "bg-red-500" },
        2: { score: 2, label: "Fair", color: "bg-amber-500" },
        3: { score: 3, label: "Good", color: "bg-lime-500" },
        4: { score: 4, label: "Strong", color: "bg-emerald-500" },
    };
    return map[raw as 1 | 2 | 3 | 4];
}
