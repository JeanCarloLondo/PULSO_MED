const smokeDb = db.getSiblingDB("pulsomed");
const coll = smokeDb.getCollection("_sprint0_smoke");

try {
  db.adminCommand("ping");
  print("OK: MongoDB responde.");

  coll.deleteMany({});
  coll.insertMany([
    { fuente: "siata", pm25: 32.5, ok: true },
    { fuente: "metro", afluencia: 1280, ok: true },
  ]);

  const nDocs = coll.countDocuments({ ok: true });
  if (nDocs !== 2) {
    print(`ERROR: se esperaban 2 docs, se leyeron ${nDocs}.`);
    quit(2);
  }

  const sample = coll.findOne({ fuente: "siata" });
  print(`OK: insercion y lectura OK. Doc de muestra: ${JSON.stringify(sample)}`);

  coll.drop();
  print("OK: coleccion de prueba eliminada.");
  print("OK: Smoke MongoDB paso.");
} catch (err) {
  print(`ERROR: Smoke MongoDB fallo: ${err}`);
  quit(1);
}
