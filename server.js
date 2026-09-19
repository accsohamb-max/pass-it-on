const express = require("express");
const path = require("path");
const crypto = require("crypto");
const Database = require("better-sqlite3");
const rateLimit = require("express-rate-limit");

const app = express();

/* Render runs behind a proxy. */
app.set("trust proxy", 1);

const PORT = process.env.PORT || 3000;

/* DATABASE */
const DB_PATH =
  process.env.DB_PATH ||
  path.join(__dirname, "pass-it-on.db");

const db = new Database(DB_PATH);

db.pragma("journal_mode = WAL");

/* TABLES */
db.exec(`
  CREATE TABLE IF NOT EXISTS chains (
    id TEXT PRIMARY KEY,
    prompt TEXT NOT NULL DEFAULT 'Add anything ✨',
    created_at TEXT NOT NULL,
    visibility TEXT NOT NULL DEFAULT 'public',
    access_code_hash TEXT
  );

  CREATE TABLE IF NOT EXISTS contributions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id TEXT NOT NULL,
    text TEXT NOT NULL,
    created_at TEXT NOT NULL,
    contributor_id INTEGER,
    FOREIGN KEY (chain_id) REFERENCES chains(id)
  );

  CREATE INDEX IF NOT EXISTS idx_contributions_chain
  ON contributions(chain_id);

  CREATE INDEX IF NOT EXISTS idx_chains_created
  ON chains(created_at);

  CREATE TABLE IF NOT EXISTS chain_participants (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id TEXT NOT NULL,
    token TEXT NOT NULL,
    contributor_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(chain_id, token),
    UNIQUE(chain_id, contributor_id),
    FOREIGN KEY (chain_id) REFERENCES chains(id)
  );

  CREATE INDEX IF NOT EXISTS idx_chain_participants_chain
  ON chain_participants(chain_id);
`);

/* MIGRATIONS FOR EXISTING DATABASES */

try {
  db.exec(`
    ALTER TABLE chains
    ADD COLUMN visibility TEXT NOT NULL DEFAULT 'public'
  `);
} catch {}

try {
  db.exec(`
    ALTER TABLE chains
    ADD COLUMN access_code_hash TEXT
  `);
} catch {}

try {
  db.exec(`
    ALTER TABLE contributions
    ADD COLUMN contributor_id INTEGER
  `);
} catch {}

/* EXISTING CONTRIBUTIONS GET STABLE FALLBACK IDs */

db.prepare(`
  UPDATE contributions
  SET contributor_id = id
  WHERE contributor_id IS NULL
`).run();

/* RATE LIMIT */

const apiLimiter = rateLimit({
  windowMs: 60 * 1000,
  limit: 60,
  standardHeaders: "draft-7",
  legacyHeaders: false
});

app.use(
  express.json({
    limit: "10kb"
  })
);

app.use(
  "/api",
  apiLimiter
);

/* STATIC FILES */

app.use(
  express.static(
    path.join(
      __dirname,
      "public"
    )
  )
);

/* =========================================================
   HELPERS
   ========================================================= */

function makeId() {
  return crypto
    .randomBytes(5)
    .toString("base64url");
}

function cleanText(value) {
  if (
    typeof value !== "string"
  ) {
    return "";
  }

  return value
    .trim()
    .slice(0, 200);
}

function cleanPrompt(value) {
  const allowed = [
    "Make someone laugh 😂",
    "Drop a random thought 🧠",
    "Add a song 🎵",
    "Tell us something 🤫",
    "Say something to the world 🌎",
    "Add anything ✨"
  ];

  return allowed.includes(value)
    ? value
    : "Add anything ✨";
}

function cleanToken(value) {
  if (
    typeof value !== "string"
  ) {
    return "";
  }

  return value
    .trim()
    .slice(0, 100);
}

function cleanVisibility(value) {
  if (
    value === "private"
  ) {
    return "private";
  }

  if (
    value === "link" ||
    value === "link-only"
  ) {
    return "link";
  }

  return "public";
}

/* =========================================================
   PRIVATE CHAINS
   ========================================================= */

function makeAccessCode() {
  const alphabet =
    "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

  const bytes =
    crypto.randomBytes(6);

  let code = "";

  for (
    let i = 0;
    i < 6;
    i++
  ) {
    code +=
      alphabet[
        bytes[i] %
        alphabet.length
      ];
  }

  return code;
}

function hashAccessCode(
  code
) {
  return crypto
    .createHash("sha256")
    .update(
      String(
        code || ""
      )
        .trim()
        .toUpperCase()
    )
    .digest("hex");
}

function cleanAccessCode(
  code
) {
  if (
    typeof code !== "string"
  ) {
    return "";
  }

  return code
    .trim()
    .toUpperCase()
    .replace(
      /[^A-Z0-9]/g,
      ""
    )
    .slice(0, 12);
}

function hasPrivateAccess(
  chain,
  code
) {
  if (
    chain.visibility !==
    "private"
  ) {
    return true;
  }

  const clean =
    cleanAccessCode(
      code
    );

  if (
    !clean ||
    !chain.access_code_hash
  ) {
    return false;
  }

  return (
    hashAccessCode(
      clean
    ) ===
    chain.access_code_hash
  );
}

function getChainAccessCode(
  req
) {
  return cleanAccessCode(
    req.get(
      "X-Chain-Code"
    ) ||
      req.query?.code ||
      ""
  );
}

/* =========================================================
   CONTRIBUTOR IDs
   ========================================================= */

const getParticipant =
  db.prepare(`
    SELECT
      contributor_id
        AS contributorId

    FROM chain_participants

    WHERE
      chain_id = ?
      AND token = ?
  `);

function ensureParticipant(
  chainId,
  token
) {
  const clean =
    cleanToken(
      token
    );

  if (!clean) {
    return null;
  }

  const existing =
    getParticipant.get(
      chainId,
      clean
    );

  if (existing) {
    return (
      existing.contributorId
    );
  }

  const nextId =
    db.prepare(`
      SELECT
        COALESCE(
          MAX(contributor_id),
          0
        ) + 1
        AS nextId

      FROM chain_participants

      WHERE chain_id = ?
    `).get(
      chainId
    ).nextId;

  db.prepare(`
    INSERT INTO
      chain_participants
      (
        chain_id,
        token,
        contributor_id,
        created_at
      )

    VALUES (
      ?,
      ?,
      ?,
      ?
    )
  `).run(
    chainId,
    clean,
    nextId,
    new Date()
      .toISOString()
  );

  return nextId;
}

/* =========================================================
   LIVE SSE
   ========================================================= */

/*
  Presence is tracked by contributor token,
  not raw SSE connections.

  One person can briefly have multiple
  EventSource connections while a browser
  reconnects.

  Those connections still count as
  ONE live person.
*/

const liveConnections =
  new Map();

function getLiveConnections(
  chainId
) {
  if (
    !liveConnections.has(
      chainId
    )
  ) {
    liveConnections.set(
      chainId,
      new Map()
    );
  }

  return (
    liveConnections.get(
      chainId
    )
  );
}

function getLiveCount(
  chainId
) {
  const people =
    liveConnections.get(
      chainId
    );

  return people
    ? people.size
    : 0;
}

function addLiveConnection(
  chainId,
  token,
  res
) {
  const people =
    getLiveConnections(
      chainId
    );

  if (
    !people.has(
      token
    )
  ) {
    people.set(
      token,
      new Set()
    );
  }

  people
    .get(token)
    .add(res);
}

function removeLiveConnection(
  chainId,
  token,
  res
) {
  const people =
    liveConnections.get(
      chainId
    );

  if (!people) {
    return false;
  }

  const connections =
    people.get(
      token
    );

  if (!connections) {
    return false;
  }

  const removed =
    connections.delete(
      res
    );

  if (
    connections.size ===
    0
  ) {
    people.delete(
      token
    );
  }

  if (
    people.size === 0
  ) {
    liveConnections.delete(
      chainId
    );
  }

  return removed;
}

function broadcastPresence(
  chainId
) {
  const people =
    liveConnections.get(
      chainId
    );

  if (!people) {
    return;
  }

  const payload =
    JSON.stringify({
      type:
        "presence",

      count:
        people.size
    });

  const deadConnections =
    [];

  for (
    const [
      token,
      connections
    ] of people.entries()
  ) {
    for (
      const res
      of connections
    ) {
      try {
        res.write(
          `data: ${payload}\n\n`
        );
      }

      catch {
        deadConnections.push({
          chainId,
          token,
          res
        });
      }
    }
  }

  for (
    const item
    of deadConnections
  ) {
    const {
      chainId:
        deadChainId,

      token:
        deadToken,

      res
    } = item;

    removeLiveConnection(
      deadChainId,
      deadToken,
      res
    );
  }
}

function broadcastContribution(
  chainId,
  contribution,
  count
) {
  const people =
    liveConnections.get(
      chainId
    );

  if (!people) {
    return;
  }

  const payload =
    JSON.stringify({
      type:
        "contribution",

      contribution,

      count
    });

  for (
    const connections
    of people.values()
  ) {
    for (
      const res
      of connections
    ) {
      try {
        res.write(
          `data: ${payload}\n\n`
        );
      }

      catch {
        /*
          Cleanup happens
          when the request closes.
        */
      }
    }
  }
}

/* =========================================================
   LIVE EVENTS
   ========================================================= */

app.get(
  "/api/chains/:id/events",

  (req, res) => {

    const chainId =
      req.params.id;

    const chainMeta =
      db.prepare(`
        SELECT
          visibility,
          access_code_hash

        FROM chains

        WHERE id = ?
      `).get(
        chainId
      );

    if (!chainMeta) {
      return res
        .status(404)
        .json({
          error:
            "Chain not found."
        });
    }

    if (
      chainMeta.visibility ===
        "private" &&
      !hasPrivateAccess(
        chainMeta,
        getChainAccessCode(
          req
        )
      )
    ) {
      return res
        .status(401)
        .json({
          error:
            "PRIVATE_CHAIN"
        });
    }

    const token =
      typeof req.query
        .token ===
      "string"

        ? req.query
            .token
            .trim()
            .slice(
              0,
              100
            )

        : "";

    if (!token) {
      return res
        .status(400)
        .json({
          error:
            "Participant token is required."
        });
    }

    const chain =
      db.prepare(
        `
          SELECT id
          FROM chains
          WHERE id = ?
        `
      ).get(
        chainId
      );

    if (!chain) {
      return res
        .status(404)
        .json({
          error:
            "Chain not found."
        });
    }

    /* SSE HEADERS */

    res.setHeader(
      "Content-Type",
      "text/event-stream"
    );

    res.setHeader(
      "Cache-Control",
      "no-cache, no-transform"
    );

    res.setHeader(
      "Connection",
      "keep-alive"
    );

    res.setHeader(
      "X-Accel-Buffering",
      "no"
    );

    if (
      res.flushHeaders
    ) {
      res.flushHeaders();
    }

    /* REGISTER */

    addLiveConnection(
      chainId,
      token,
      res
    );

    /* CONNECTED EVENT */

    res.write(
      `data: ${JSON.stringify({
        type:
          "connected",

        presenceCount:
          getLiveCount(
            chainId
          )
      })}\n\n`
    );

    broadcastPresence(
      chainId
    );

    /* HEARTBEAT */

    const heartbeat =
      setInterval(
        () => {

          try {
            res.write(
              `: heartbeat\n\n`
            );
          }

          catch {
            clearInterval(
              heartbeat
            );

            const removed =
              removeLiveConnection(
                chainId,
                token,
                res
              );

            if (
              removed
            ) {
              broadcastPresence(
                chainId
              );
            }
          }

        },
        25000
      );

    /* CLEANUP */

    req.on(
      "close",
      () => {

        clearInterval(
          heartbeat
        );

        const removed =
          removeLiveConnection(
            chainId,
            token,
            res
          );

        if (
          removed
        ) {
          broadcastPresence(
            chainId
          );
        }
      }
    );
  }
);

/* =========================================================
   CREATE CHAIN
   ========================================================= */

const createChain =
  db.transaction(
    (
      prompt,
      text,
      token,
      visibility
    ) => {

      let id;

      do {
        id =
          makeId();

      } while (
        db.prepare(`
          SELECT 1
          FROM chains
          WHERE id = ?
        `).get(
          id
        )
      );

      const now =
        new Date()
          .toISOString();

      let accessCode =
        null;

      let accessCodeHash =
        null;

      if (
        visibility ===
        "private"
      ) {
        accessCode =
          makeAccessCode();

        accessCodeHash =
          hashAccessCode(
            accessCode
          );
      }

      /*
        IMPORTANT:
        Create chain FIRST.

        chain_participants has a
        foreign key pointing to chains,
        so ensureParticipant must happen
        AFTER this insert.
      */

      db.prepare(`
        INSERT INTO chains
        (
          id,
          prompt,
          created_at,
          visibility,
          access_code_hash
        )

        VALUES (
          ?,
          ?,
          ?,
          ?,
          ?
        )
      `).run(
        id,
        prompt,
        now,
        visibility,
        accessCodeHash
      );

      const contributorId =
        ensureParticipant(
          id,
          token
        );

      db.prepare(`
        INSERT INTO
          contributions
        (
          chain_id,
          text,
          created_at,
          contributor_id
        )

        VALUES (
          ?,
          ?,
          ?,
          ?
        )
      `).run(
        id,
        text,
        now,
        contributorId
      );

      return {
        id,
        contributorId,
        accessCode
      };
    }
  );

/* CREATE NEW CHAIN */

app.post(
  "/api/chains",

  (req, res) => {

    const text =
      cleanText(
        req.body?.text
      );

    const prompt =
      cleanPrompt(
        req.body?.prompt
      );

    const token =
      cleanToken(
        req.body?.token
      );

    const visibility =
      cleanVisibility(
        req.body?.visibility
      );

    if (!text) {
      return res
        .status(400)
        .json({
          error:
            "Contribution is required."
        });
    }

    if (!token) {
      return res
        .status(400)
        .json({
          error:
            "Participant token is required."
        });
    }

    const created =
      createChain(
        prompt,
        text,
        token,
        visibility
      );

    res
      .status(201)
      .json({

        id:
          created.id,

        contributorId:
          created
            .contributorId,

        prompt,

        visibility,

        accessCode:
          created
            .accessCode ||
          undefined,

        url:
          `/c/${created.id}`
      });
  }
);

/* =========================================================
   GET CHAIN
   ========================================================= */

app.get(
  "/api/chains/:id",

  (req, res) => {

    const chain =
      db.prepare(`
        SELECT
          id,
          prompt,
          created_at,
          visibility,
          access_code_hash

        FROM chains

        WHERE id = ?
      `).get(
        req.params.id
      );

    if (!chain) {
      return res
        .status(404)
        .json({
          error:
            "Chain not found."
        });
    }

    const count =
      db.prepare(`
        SELECT
          COUNT(*) AS count

        FROM contributions

        WHERE chain_id = ?
      `).get(
        req.params.id
      ).count;

    if (
      chain.visibility ===
        "private" &&
      !hasPrivateAccess(
        chain,
        getChainAccessCode(
          req
        )
      )
    ) {
      return res
        .status(401)
        .json({

          error:
            "PRIVATE_CHAIN",

          prompt:
            chain.prompt,

          visibility:
            "private",

          count
        });
    }

    const contributions =
      db.prepare(`
        SELECT
          id,
          text,

          contributor_id
            AS contributorId,

          created_at
            AS createdAt

        FROM contributions

        WHERE chain_id = ?

        ORDER BY id ASC
      `).all(
        req.params.id
      );

    const first =
      contributions[0];

    const last =
      contributions[
        contributions.length -
        1
      ];

    res.json({

      id:
        chain.id,

      prompt:
        chain.prompt,

      visibility:
        chain.visibility ===
        "link-only"

          ? "link"

          : chain.visibility,

      createdAt:
        chain.created_at,

      contributions,

      stats: {

        count,

        firstContribution:
          first?.createdAt ||
          chain.created_at,

        latestContribution:
          last?.createdAt ||
          chain.created_at
      }
    });
  }
);

/* =========================================================
   ADD CONTRIBUTION
   ========================================================= */

app.post(
  "/api/chains/:id/contributions",

  (req, res) => {

    const chain =
      db.prepare(`
        SELECT id
        FROM chains
        WHERE id = ?
      `).get(
        req.params.id
      );

    if (!chain) {
      return res
        .status(404)
        .json({
          error:
            "Chain not found."
        });
    }

    const chainMeta =
      db.prepare(`
        SELECT
          visibility,
          access_code_hash

        FROM chains

        WHERE id = ?
      `).get(
        req.params.id
      );

    if (
      chainMeta
        ?.visibility ===
        "private" &&

      !hasPrivateAccess(
        chainMeta,
        getChainAccessCode(
          req
        )
      )
    ) {
      return res
        .status(401)
        .json({
          error:
            "PRIVATE_CHAIN"
        });
    }

    const text =
      cleanText(
        req.body?.text
      );

    const token =
      cleanToken(
        req.body?.token
      );

    if (!text) {
      return res
        .status(400)
        .json({
          error:
            "Contribution is required."
        });
    }

    if (!token) {
      return res
        .status(400)
        .json({
          error:
            "Participant token is required."
        });
    }

    const contributorId =
      ensureParticipant(
        req.params.id,
        token
      );

    const count =
      db.prepare(`
        SELECT
          COUNT(*) AS count

        FROM contributions

        WHERE chain_id = ?
      `).get(
        req.params.id
      ).count;

    if (
      count >= 1000
    ) {
      return res
        .status(409)
        .json({
          error:
            "This chain has reached its 1,000 contribution limit."
        });
    }

    const now =
      new Date()
        .toISOString();

    const result =
      db.prepare(`
        INSERT INTO
          contributions
        (
          chain_id,
          text,
          created_at,
          contributor_id
        )

        VALUES (
          ?,
          ?,
          ?,
          ?
        )
      `).run(
        req.params.id,
        text,
        now,
        contributorId
      );

    const contribution = {

      id:
        result
          .lastInsertRowid,

      text,

      contributorId,

      createdAt:
        now
    };

    const newCount =
      count + 1;

    /*
      Broadcast only after
      SQLite successfully saves it.
    */

    broadcastContribution(
      req.params.id,
      contribution,
      newCount
    );

    res
      .status(201)
      .json({

        id:
          result
            .lastInsertRowid,

        contribution,

        count:
          newCount
      });
  }
);

/* =========================================================
   DISCOVER
   PUBLIC + PRIVATE
   LINK-ONLY STAYS HIDDEN
   ========================================================= */

app.get(
  "/api/discover",

  (req, res) => {

    const chains =
      db.prepare(`
        SELECT
          c.id,

          c.prompt,

          c.created_at
            AS createdAt,

          c.visibility,

          COUNT(co.id)
            AS count,

          MAX(co.created_at)
            AS latestContribution,

          CASE

            WHEN
              c.visibility =
              'private'

            THEN NULL

            ELSE (

              SELECT
                co2.text

              FROM
                contributions co2

              WHERE
                co2.chain_id =
                c.id

              ORDER BY
                co2.id DESC

              LIMIT 1
            )

          END AS preview

        FROM chains c

        LEFT JOIN
          contributions co

          ON
            co.chain_id =
            c.id

        WHERE
          c.visibility
          IN (
            'public',
            'private'
          )

        GROUP BY
          c.id

        ORDER BY
          latestContribution
          DESC

        LIMIT 100
      `).all();

    res.json({

      chains:
        chains.map(
          chain => ({

            ...chain,

            locked:
              chain.visibility ===
              "private"

          })
        )
    });
  }
);

/* =========================================================
   PAGES
   ========================================================= */

app.get(
  "/",

  (req, res) => {

    res.sendFile(
      path.join(
        __dirname,
        "public",
        "index.html"
      )
    );
  }
);

app.get(
  "/discover",

  (req, res) => {

    res.sendFile(
      path.join(
        __dirname,
        "public",
        "discover.html"
      )
    );
  }
);

app.get(
  "/c/:id",

  (req, res) => {

    res.sendFile(
      path.join(
        __dirname,
        "public",
        "index.html"
      )
    );
  }
);

/* =========================================================
   START
   ========================================================= */

app.listen(
  PORT,

  () => {

    console.log(
      `Pass It On running at http://localhost:${PORT}`
    );

    console.log(
      `Database: ${DB_PATH}`
    );

    console.log(
      `Discover running at http://localhost:${PORT}/discover`
    );
  }
);