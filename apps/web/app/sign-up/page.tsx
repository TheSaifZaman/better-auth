import Link from "next/link";
import { AuthShell, Signupform } from "../../components/auth-form";

export default function SignUpPage() {
    return (
        <AuthShell
            footer={
                <>
                    Already have an account?{" "}
                    <Link href="/sign-in" className="font-medium text-indigo-600 hover:text-indigo-700">
                        Sign in
                    </Link>
                </>
            }
        >
            <Signupform />
        </AuthShell>
    );
}
