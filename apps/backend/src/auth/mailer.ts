import { createTransport, type Transporter } from 'nodemailer';

/**
 * Shared mail transport. Defaults to Mailpit (SMTP on localhost:1025,
 * web UI on http://localhost:8025) for local development. Point the
 * SMTP_* env vars at a real provider in production.
 */
const transporter: Transporter = createTransport({
  host: process.env.SMTP_HOST ?? 'localhost',
  port: Number(process.env.SMTP_PORT ?? 1025),
  secure: process.env.SMTP_SECURE === 'true',
  auth: process.env.SMTP_USER
    ? {
        user: process.env.SMTP_USER,
        pass: process.env.SMTP_PASS,
      }
    : undefined,
});

const FROM = process.env.MAIL_FROM ?? 'Better Auth <no-reply@betterauth.local>';

export async function sendMail(options: {
  to: string;
  subject: string;
  text: string;
  html: string;
}): Promise<void> {
  await transporter.sendMail({ from: FROM, ...options });
}

function layout(title: string, body: string, cta: { href: string; label: string }): string {
  return `
  <div style="font-family:ui-sans-serif,system-ui,sans-serif;max-width:480px;margin:0 auto;padding:32px 24px;color:#0f172a">
    <p style="font-family:ui-monospace,monospace;font-size:11px;letter-spacing:.25em;text-transform:uppercase;color:#4f46e5;margin:0 0 8px">Better&#8209;Auth</p>
    <h1 style="font-size:20px;margin:0 0 12px">${title}</h1>
    <p style="font-size:14px;line-height:1.6;color:#475569;margin:0 0 24px">${body}</p>
    <a href="${cta.href}" style="display:inline-block;background:#4f46e5;color:#fff;font-size:14px;font-weight:600;text-decoration:none;padding:10px 20px;border-radius:8px">${cta.label}</a>
    <p style="font-size:12px;color:#94a3b8;margin:24px 0 0">If the button doesn't work, paste this link into your browser:<br><span style="word-break:break-all">${cta.href}</span></p>
  </div>`;
}

export async function sendVerificationEmail(to: string, url: string): Promise<void> {
  await sendMail({
    to,
    subject: 'Verify your email',
    text: `Verify your email address by visiting: ${url}`,
    html: layout(
      'Verify your email',
      'Confirm this address to activate your account. This link expires in 1 hour.',
      { href: url, label: 'Verify email' },
    ),
  });
}

export async function sendResetPasswordEmail(to: string, url: string): Promise<void> {
  await sendMail({
    to,
    subject: 'Reset your password',
    text: `Reset your password by visiting: ${url}`,
    html: layout(
      'Reset your password',
      "We received a request to reset your password. If this wasn't you, you can ignore this email. This link expires in 1 hour.",
      { href: url, label: 'Reset password' },
    ),
  });
}
