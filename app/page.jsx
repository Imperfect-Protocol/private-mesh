import GridClient from "./GridClient";

export default function HomePage() {
  return (
    <main className="grid-page">
      <div className="grid-page__matrix" />
      <section className="grid-shell">
        <GridClient />
      </section>
    </main>
  );
}
