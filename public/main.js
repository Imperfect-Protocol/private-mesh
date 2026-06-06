fetch('/mesh.json')
  .then((response) => response.json())
  .then((mesh) => {
    globalThis.privateMesh = mesh;
    console.info('[mesh] ready', {
      version: mesh.version,
      endpoints: mesh.endpoints,
    });
  })
  .catch((error) => {
    console.warn('[mesh] status unavailable', error);
  });
