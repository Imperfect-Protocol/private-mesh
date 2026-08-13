import "./globals.css";

export const metadata = {
  title: "The Grid | Private Mesh",
  description: "Private Mesh peer bootstrap telemetry."
};

export default function RootLayout({ children }) {
  return (
    <html lang="en">
      <body>{children}</body>
    </html>
  );
}
