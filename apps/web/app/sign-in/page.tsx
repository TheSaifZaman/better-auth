import Link from "next/link";
import { AuthShell, LoginForm } from "../../components/auth-form";

export default function SignInPage() {
    return (
        <AuthShell
            footer={
                <>
                    New here?{" "}
                    <Link href="/sign-up" className="font-medium text-indigo-600 hover:text-indigo-700">
                        Create an account
                    </Link>
                </>
            }
        >
            <LoginForm />
        </AuthShell>
    );
}
