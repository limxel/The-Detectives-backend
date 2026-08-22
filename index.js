const express = require('express');
const http = require('http');
const { Server } = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
    cors: { origin: "*" }
});

const rooms = {};

const MIN_PLAYERS = 5;
const MAX_PLAYERS = 12;

// KEEP IN SYNC WITH frontend/src/App.jsx — this is the server-authoritative
// roster used by generateForensicClue and the in-game clue logic.
const CHARACTERS = [
    { name: 'Creed', url: 'https://i.postimg.cc/xjJPpJNY/2fdb739d-84b9-4d0d-8170-d281954a8b7c.jpg', realName: 'Creed Vance', height: '188 cm', weight: '85 kg', bloodType: 'A+', hobbies: 'Bidding on multi-million dollar digital art auctions, collecting physical vintage luxury watches, drinking rare red wine.' },
    { name: 'Karl', url: 'https://i.postimg.cc/nVj1Sj9c/4169de45-bccb-49a2-bea0-6e41091a9453.jpg', realName: 'Karl Thorne', height: '178 cm', weight: '68 kg', bloodType: 'A+', hobbies: 'Practicing classical fencing with custom steel foils, attending grand opera premieres, studying royal family genealogy.' },
    { name: 'Anthonio', url: 'https://i.postimg.cc/VsrFZr0z/53b8e637-00ec-4baa-bdb8-4bdb01564a54.jpg', realName: 'Anthonio Rossi', height: '185 cm', weight: '110 kg', bloodType: 'O+', hobbies: 'Feeding wild ducks in his pool, smoking expensive imported cigars, managing garbage disposal contracts.' },
    { name: 'James', url: 'https://i.postimg.cc/1RnK7nVm/61da301d-4838-4b30-9e52-45146fe8882d.jpg', realName: 'James Creed', height: '183 cm', weight: '82 kg', bloodType: 'B+', hobbies: 'Shredding heavy metal riffs on a custom black Explorer guitar, restoring vintage V8 muscle cars, collecting hunting rifles.' },
    { name: 'Cedric', url: 'https://i.postimg.cc/Kck5pk3c/6684a55e-28ee-44cb-9ecc-554d44540f8c.jpg', realName: 'Cedric Rostova', height: '180 cm', weight: '84 kg', bloodType: 'O+', hobbies: 'Prison-style heavy calisthenics, sketching monochrome tattoo designs, carving makeshift tools out of spare scrap metal.' },
    { name: 'Lidy', url: 'https://i.postimg.cc/gcXKtXLJ/74409a55-7ddb-4fae-bc0a-0d21aa1ae1be.jpg', realName: 'Lidy Vance', height: '168 cm', weight: '62 kg', bloodType: 'A-', hobbies: 'Baking homemade cherry pies, knitting wool sweaters, reading paperback detective novels.' },
    { name: 'May', url: 'https://i.postimg.cc/c1gckgtC/a7af6a30-acf5-4e55-b9ed-4ed5080008fe.jpg', realName: 'May Creed', height: '152 cm', weight: '42 kg', bloodType: 'B+', hobbies: 'Picking glowing bioluminescent forest mushrooms, crafting flower crowns, collecting sparkling dust in small glass jars.' },
    { name: 'Gregory', url: 'https://i.postimg.cc/dQk9NkZD/aa60cee7-e3c9-460a-943a-70f505b8526a.jpg', realName: 'Dr. Gregory Chen', height: '182 cm', weight: '75 kg', bloodType: 'A-', hobbies: 'Practicing micro-stitch sewing on synthetic skin, collecting historical surgical scalpels, studying forensic human anatomy.' },
    { name: 'Onyx', url: 'https://i.postimg.cc/BZLC7LPX/c218fab0-360a-4de0-9fcf-cc7cd2180f9d.jpg', realName: 'Onyx Grey', height: '174 cm', weight: '58 kg', bloodType: 'B-', hobbies: 'Writing melancholic poetry in a locked black journal, collecting midwest emo vinyl records, hanging out alone in dark, rainy places.' },
    { name: 'Max', url: 'https://i.postimg.cc/2jbx9bLq/f11eb0a7-dda4-4845-8793-2daac4d4bbf0.jpg', realName: 'Max Tanaka', height: '176 cm', weight: '64 kg', bloodType: 'B-', hobbies: 'Drawing chaotic, repetitive patterns on walls with charcoal, talking to invisible visitors, hoarding colorful prescription pills.' },
    { name: 'Bea', url: 'https://i.postimg.cc/JzvwXxK8/54afbcca-49b7-4570-8b15-38936afa1975.jpg', realName: 'Bea Gray', height: '180 cm', weight: '70 kg', bloodType: 'A+', hobbies: 'Training sword strikes with an authentic Japanese katana, updating a personal handwritten revenge checklist, intense martial arts meditation.' },
    { name: 'Moonka', url: 'https://i.postimg.cc/ht6FmsMF/28b49642-1595-46cb-b607-f0b7fbce307a.jpg', realName: 'Moonka Miller', height: '164 cm', weight: '53 kg', bloodType: 'O+', hobbies: 'Writing deep metaphorical poems for a literature club, playing sad classical piano melodies, writing script codes.' }
];

// Active countdown intervals, keyed by roomId. Kept SEPARATE from the room object
// so a Timeout object never accidentally gets sent to clients via io.emit(room).
const roomTimers = {};

// Safety timers for waiting on players to load, keyed by roomId. Previously, roles
// were assigned (assignRoles) only once ABSOLUTELY ALL players sent 'game_loaded'.
// If even one player's tab was backgrounded (browsers throttle setTimeout hard in
// background tabs), their game_loaded could arrive late or never — and then roles
// were never assigned to ANYONE, while other clients' local watchdog silently moved
// them into the role-reveal phase, leaving them stuck forever on "Decrypting
// identity..." because the server never actually issued roles.
// Now: if not everyone has loaded within LOAD_TIMEOUT_MS, roles are still assigned
// to the room's current player list and the intro starts anyway. This guarantees
// roles are always assigned.
//
// This same object is reused for a second, similar wait — confirmation from all
// players that they've finished watching the role reveal screen (see
// 'role_reveal_done'), under the key `${roomId}_role`, to avoid a duplicate dict.
const roomLoadTimers = {};
const LOAD_TIMEOUT_MS = 15000;

// Duration of a single player's turn during the action phase (ms), used while the
// player hasn't picked a room to search yet. Timed STRICTLY on the server — the
// client only visualizes the countdown from the sent endsAt, so there's no point
// spoofing a local timer to stretch a turn longer than allowed.
const TURN_DURATION_MS = 30000;

// Once the active player has picked a room to search (see 'select_room'), their
// turn no longer runs for the full TURN_DURATION_MS — the action for the turn is
// already done (moving to the room + seeing who else is there), so the server
// gives them a short grace window to actually read the room's contents, then
// AUTOMATICALLY ends the turn and advances the queue. This is authoritative on
// the server (not just a client-side timer) so a stalled/closed client tab can't
// hang the room waiting on a turn that's effectively already over.
const ROOM_INSPECT_MS = 4500;

// A player locked in the Holding Cell for the round (see game.
// lockedInHoldingCellPlayerId / startNewRound) never actually gets to act on
// the turn that comes up for them — see startPlayerTurn's early-return branch
// below. Rather than run the normal TURN_DURATION_MS countdown (which would
// have to be silently skippable, and would show a live timer nobody can ever
// use), their "turn" is announced and then auto-advanced after this short,
// fixed beat — purely pacing, never shown to anyone as a countdown.
const HOLDING_CELL_SKIP_DELAY_MS = 900;

// Strict duration for the trial phase. The server owns this countdown and
// resolves the vote automatically when it expires.
const TRIAL_DURATION_MS = 120000;
const TRIAL_BLACKOUT_MS = 1500;
// Was 2000ms — just long enough to flash "TRIAL PHASE COMMENCING". Now this
// screen also carries the findings recap (bodies/clues discovered so far,
// see buildFindingsSummary), so it needs real reading time.
const TRIAL_ANNOUNCEMENT_MS = 6000;
const TRIAL_RESOLUTION_MS = 3500;

// How long the GAME_OVER summary screen (winner, roles, etc.) stays up before
// the server automatically resets the room and sends everyone back to the
// lobby to start a new match.
const GAME_OVER_SUMMARY_MS = 8000;

// Active setTimeouts for auto-ending the current turn (or advancing after the
// trial phase), keyed by roomId.
const roomTurnTimers = {};
const trialTickTimers = {};
const phaseTimelineTimers = {};
// Independent watchdogs for the short transition into a trial.  Keeping these
// separate from the timeline timeout means the fallback still runs if a
// timeline callback throws before it can schedule its successor.
const trialTransitionSafetyTimers = {};

// Auto-advance out of the GAME_OVER summary screen and back to the lobby,
// keyed by roomId. Kept separate from the other timer dicts since it belongs
// to a completely different part of the lifecycle (post-match, not mid-match).
const gameOverTimers = {};

function clearTurnTimer(roomId) {
    if (roomTurnTimers[roomId]) {
        clearTimeout(roomTurnTimers[roomId]);
        delete roomTurnTimers[roomId];
    }
}

function clearTrialTickTimer(roomId) {
    if (trialTickTimers[roomId]) {
        clearInterval(trialTickTimers[roomId]);
        delete trialTickTimers[roomId];
    }
}

function clearPhaseTimelineTimer(roomId) {
    if (phaseTimelineTimers[roomId]) {
        clearTimeout(phaseTimelineTimers[roomId]);
        delete phaseTimelineTimers[roomId];
    }
}

function clearTrialTransitionSafetyTimer(roomId) {
    if (trialTransitionSafetyTimers[roomId]) {
        clearTimeout(trialTransitionSafetyTimers[roomId]);
        delete trialTransitionSafetyTimers[roomId];
    }
}

function clearTrialTransitionTimers(roomId) {
    clearPhaseTimelineTimer(roomId);
    clearTrialTransitionSafetyTimer(roomId);
}

function clearGameOverTimer(roomId) {
    if (gameOverTimers[roomId]) {
        clearTimeout(gameOverTimers[roomId]);
        delete gameOverTimers[roomId];
    }
}

function phaseStatePayload(targetRoom) {
    const game = targetRoom.game;
    const trial = game?.trial;
    return {
        code: targetRoom.code,
        phase: game?.timelinePhase || 'EXPLORATION',
        phaseStartTime: game?.phaseStartTime || Date.now(),
        round: game?.round || 0,
        trial: trial ? {
            status: trial.status, endsAt: trial.endsAt,
            eligibleVoterIds: trial.eligibleVoterIds || [],
            candidates: (trial.candidates || []).map(({ id, nickname, character }) => ({ id, nickname, character })),
            // Vote targets and totals are private until the resolution phase.
            confirmedVoterIds: trial.confirmedVoterIds || [],
            result: trial.status === 'resolved' ? trial.result || null : null
        } : null,
        players: trial ? buildTrialRoster(targetRoom) : undefined,
        // Only worth computing/sending once the case is actually being
        // presented — no reason to pay for it during plain exploration.
        findings: (game?.timelinePhase && game.timelinePhase !== 'EXPLORATION')
            ? buildFindingsSummary(targetRoom)
            : undefined,
        // Public: whoever is confined to the Holding Cell for the CURRENT
        // round (see game.lockedInHoldingCellPlayerId / startNewRound) — sent
        // here too, not just on 'round_start', so a reconnecting client picks
        // it back up without waiting for the next round.
        lockedInHoldingCell: game?.lockedInHoldingCellPlayerId
            ? {
                id: game.lockedInHoldingCellPlayerId,
                nickname: (targetRoom.players.find(p => p.id === game.lockedInHoldingCellPlayerId) || {}).nickname || 'Agent'
            }
            : null
    };
}

function broadcastPhaseState(targetRoom) {
    io.to(targetRoom.id).emit('phase_state', phaseStatePayload(targetRoom));
}

function sendPhaseState(socket, targetRoom) {
    socket.emit('phase_state', phaseStatePayload(targetRoom));
}

// `room_updated` is also emitted when a player leaves. Do not let that general
// purpose event bypass the anonymous trial payload while voting is in progress.
function roomUpdatedPayload(targetRoom) {
    const trial = targetRoom.game?.trial;
    if (!trial || trial.status !== 'voting') return targetRoom;
    return {
        ...targetRoom,
        game: {
            ...targetRoom.game,
            trial: {
                status: trial.status,
                endsAt: trial.endsAt,
                eligibleVoterIds: trial.eligibleVoterIds || [],
                candidates: (trial.candidates || []).map(({ id, nickname, character }) => ({ id, nickname, character })),
                confirmedVoterIds: trial.confirmedVoterIds || [],
                result: null
            }
        }
    };
}

function emitTrialTimer(targetRoom) {
    const trial = targetRoom.game?.trial;
    if (!trial || trial.status !== 'voting') return;

    const remaining = Math.max(0, Math.ceil((trial.endsAt - Date.now()) / 1000));
    io.to(targetRoom.id).emit('timer_update', {
        code: targetRoom.code,
        phase: 'trial',
        remaining,
        endsAt: trial.endsAt
    });
}

function getActivePlayerIds(targetRoom) {
    return (targetRoom.players || [])
        .filter(player => !player.isEliminated && !player.isObserver)
        .map(player => player.id);
}

function buildTrialVoteSummary(trial) {
    const summary = {};
    const votes = trial?.votes || {};
    Object.values(votes).forEach(targetId => {
        if (!targetId) return;
        summary[targetId] = (summary[targetId] || 0) + 1;
    });
    return summary;
}

// Builds the list of votable candidates for the trial-voting UI: one entry per
// active player, carrying just what the vote cards need to render (nickname,
// character, and their current vote tally) so the client doesn't have to
// cross-reference targetRoom.players itself.
function buildTrialCandidates(targetRoom, candidateIds, voteSummary = {}) {
    return (candidateIds || []).map(id => {
        const player = targetRoom.players.find(p => p.id === id);
        return {
            id,
            nickname: player?.nickname || 'Agent',
            character: player?.character || null,
            votes: voteSummary[id] || 0
        };
    });
}

// Keep skip votes separate from candidate votes. A `null` target is an explicit
// skip selection, never an absent/invalid candidate vote.
function buildTrialVoteBreakdown(trial, eligibleVoterIds) {
    const playerVotes = {};
    let skipVotes = 0;
    let totalVotes = 0;
    const eligible = new Set(eligibleVoterIds || []);

    Object.entries(trial?.votes || {}).forEach(([voterId, targetId]) => {
        if (!eligible.has(voterId)) return;
        totalVotes += 1;
        if (targetId) {
            playerVotes[targetId] = (playerVotes[targetId] || 0) + 1;
        } else {
            skipVotes += 1;
        }
    });

    return { playerVotes, skipVotes, totalVotes };
}

function buildTrialRoster(targetRoom) {
    return (targetRoom.players || [])
        .map(({ id, nickname, character, isEliminated, isObserver }) => ({ id, nickname, character, isEliminated: Boolean(isEliminated), isObserver: Boolean(isObserver) }));
}

// --- ENTITY POSITIONS (server-authoritative) --------------------------------
// Every occupant/body rendered inside a mansion room used to get a RANDOM
// spot picked independently by each client's own RoomVisualScene — so the
// same room looked different (players in different spots) depending on who
// was looking at it. Positions are now assigned exactly once here, the
// moment an entity (a living occupant OR a body) is first placed in a room,
// and handed out to every client as part of that entity's data. Kept on the
// room itself (not the per-round game state) and keyed by roomId -> entityId
// so a body reuses the exact same spot its living self already had (same id),
// and revisiting a room later in the match doesn't reshuffle anyone already
// positioned there.
const POSITION_X_MIN = 20, POSITION_X_MAX = 80, POSITION_Y_MIN = 48, POSITION_Y_MAX = 86;

function getEntityPosition(targetRoom, roomId, entityId) {
    if (!targetRoom.entityPositions) targetRoom.entityPositions = {};
    if (!targetRoom.entityPositions[roomId]) targetRoom.entityPositions[roomId] = {};
    const roomPositions = targetRoom.entityPositions[roomId];
    if (roomPositions[entityId]) return roomPositions[entityId];

    // Rejection-sampled against everyone already placed in this room, same
    // approach the client used to do locally — just run once, server-side,
    // so the result is identical for every viewer instead of per-client.
    const existing = Object.values(roomPositions);
    const minDistance = Math.max(12, 26 - existing.length * 1.5);
    let bestCandidate = null;
    let bestClearance = -Infinity;
    for (let attempt = 0; attempt < 24; attempt++) {
        const candidate = {
            x: POSITION_X_MIN + Math.random() * (POSITION_X_MAX - POSITION_X_MIN),
            y: POSITION_Y_MIN + Math.random() * (POSITION_Y_MAX - POSITION_Y_MIN)
        };
        const clearance = existing.reduce(
            (min, p) => Math.min(min, Math.hypot(p.x - candidate.x, p.y - candidate.y)),
            Infinity
        );
        if (clearance >= minDistance) {
            roomPositions[entityId] = candidate;
            return candidate;
        }
        if (clearance > bestClearance) {
            bestClearance = clearance;
            bestCandidate = candidate;
        }
    }
    const chosen = bestCandidate || { x: (POSITION_X_MIN + POSITION_X_MAX) / 2, y: (POSITION_Y_MIN + POSITION_Y_MAX) / 2 };
    roomPositions[entityId] = chosen;
    return chosen;
}

// Credits `player` as a finder on every EXPOSED body sitting in `roomId`,
// the moment they walk in — mirrors the credit 'search_body' already gives
// for an explicit search, just triggered by simply entering instead. Hidden
// bodies are deliberately excluded: those must stay undiscovered until a
// real 'search_body'. Returns true if this added at least one new finder.
function creditExposedBodyDiscovery(targetRoom, roomId, player, round) {
    const bodiesHere = (targetRoom.bodies || []).filter(b => b.roomId === roomId && !b.isHidden);
    let changed = false;
    bodiesHere.forEach(body => {
        if (!body.foundBy) body.foundBy = [];
        if (!body.foundBy.some(f => f.id === player.id)) {
            body.foundBy.push({ id: player.id, nickname: player.nickname, round });
            changed = true;
        }
    });
    return changed;
}

// True if the Killer has at least one victim lying somewhere in the mansion
// that nobody has found yet (foundBy empty) — hidden or exposed, doesn't
// matter, just undiscovered. Used to physically lock the Innocents' exit
// code terminal (see 'submit_innocent_code'): the case can't be closed while
// a body is still unaccounted for.
function hasUndiscoveredBody(targetRoom) {
    return (targetRoom.bodies || []).some(body => !body.foundBy || body.foundBy.length === 0);
}

const FORENSIC_CLUE_TYPES = ['bloodType', 'height', 'weight'];
const HEIGHT_CATEGORIES = ['tall', 'average', 'short'];
const WEIGHT_CATEGORIES = ['heavy', 'average', 'light'];
const FORENSIC_SHARED_COOLDOWN_ROUNDS = 1;

function heightCategory(heightStr) {
    const cm = parseInt(heightStr, 10);
    if (cm >= 182) return 'tall';
    if (cm >= 170) return 'average';
    return 'short';
}

function weightCategory(weightStr) {
    const kg = parseInt(weightStr, 10);
    if (kg >= 80) return 'heavy';
    if (kg >= 60) return 'average';
    return 'light';
}

function categoryValue(type, character) {
    if (!character) return null;
    if (type === 'bloodType') return character.bloodType;
    if (type === 'height') return heightCategory(character.height);
    return weightCategory(character.weight);
}

function isClueAmbiguousInMatch(targetRoom, type, killerCharacterName) {
    const assignedNames = (targetRoom.players || [])
        .map(p => p.character)
        .filter(Boolean);
    const killer = CHARACTERS.find(c => c.name === killerCharacterName);
    if (!killer) return false;
    const killerValue = categoryValue(type, killer);
    return assignedNames.some(name => {
        if (name === killerCharacterName) return false;
        const other = CHARACTERS.find(c => c.name === name);
        return other && categoryValue(type, other) === killerValue;
    });
}

function generateForensicClue(targetRoom, killerCharacterName, body = null) {
    if (!killerCharacterName) return null;
    const killer = CHARACTERS.find(c => c.name === killerCharacterName);
    if (!killer) return null;

    if (body && body.forensicClue) {
        return { ...body.forensicClue };
    }

    const bodyOrderSeed = ((targetRoom?.bodies || []).length + (targetRoom?.discoveredBodies || []).length);
    const chosenType = FORENSIC_CLUE_TYPES[bodyOrderSeed % FORENSIC_CLUE_TYPES.length];
    const result = { type: chosenType, value: categoryValue(chosenType, killer) };

    if (body) {
        body.forensicClue = { ...result };
    }
    return { ...result };
}

function garbleCategoryValue(categories, trueValue) {
    const others = categories.filter(c => c !== trueValue);
    return others[Math.floor(Math.random() * others.length)];
}

function resolveForensicClueForReveal(targetRoom, body) {
    if (!body) return null;
    if (body.forensicClue) {
        return { ...body.forensicClue };
    }

    const masterResult = targetRoom?.game?.forensicMasterResult;
    if (masterResult) {
        return { ...masterResult };
    }
    return null;
}

function isForensicAbilityAvailable(game, playerId, currentRound) {
    if (!game.forensicAbilityLastUsedRound) game.forensicAbilityLastUsedRound = {};
    const lastUsed = game.forensicAbilityLastUsedRound[playerId];
    if (lastUsed == null) return true;
    return (currentRound - lastUsed) > FORENSIC_SHARED_COOLDOWN_ROUNDS;
}

function markForensicAbilityUsed(game, playerId, currentRound) {
    if (!game.forensicAbilityLastUsedRound) game.forensicAbilityLastUsedRound = {};
    game.forensicAbilityLastUsedRound[playerId] = currentRound;
}

function forensicAbilityStatusPayload(targetRoom, playerId) {
    const game = targetRoom?.game;
    const lastUsed = game?.forensicAbilityLastUsedRound?.[playerId];
    if (lastUsed == null) {
        return { code: targetRoom.code, available: true, roundsRemaining: 0 };
    }

    const roundsSinceUse = game.round - lastUsed;
    const available = roundsSinceUse > FORENSIC_SHARED_COOLDOWN_ROUNDS;
    const roundsRemaining = available
        ? 0
        : Math.max(1, (FORENSIC_SHARED_COOLDOWN_ROUNDS + 1) - roundsSinceUse);

    return {
        code: targetRoom.code,
        available,
        roundsRemaining
    };
}

function emitForensicAbilityStatus(targetRoom, playerId) {
    const payload = forensicAbilityStatusPayload(targetRoom, playerId);
    io.to(playerId).emit('forensic_ability_status', payload);
    io.to(playerId).emit('forensic_verify_status', payload);
}

// Round-based cooldown gate for the Accomplice's "Set a Trap" ability (see
// 'set_trap') — same shape as isForensicAbilityAvailable/
// markForensicAbilityUsed above, just keyed under its own
// game.trapAbilityLastUsedRound so it never shares (or clashes on) the
// Forensic Examiner's counters.
function isTrapAbilityAvailable(game, playerId, currentRound) {
    if (!game.trapAbilityLastUsedRound) game.trapAbilityLastUsedRound = {};
    const lastUsed = game.trapAbilityLastUsedRound[playerId];
    if (lastUsed == null) return true;
    return (currentRound - lastUsed) > TRAP_COOLDOWN_ROUNDS;
}

function markTrapAbilityUsed(game, playerId, currentRound) {
    if (!game.trapAbilityLastUsedRound) game.trapAbilityLastUsedRound = {};
    game.trapAbilityLastUsedRound[playerId] = currentRound;
}

function trapAbilityStatusPayload(targetRoom, playerId) {
    const game = targetRoom?.game;
    const lastUsed = game?.trapAbilityLastUsedRound?.[playerId];
    if (lastUsed == null) {
        return { code: targetRoom.code, available: true, roundsRemaining: 0 };
    }

    const roundsSinceUse = game.round - lastUsed;
    const available = roundsSinceUse > TRAP_COOLDOWN_ROUNDS;
    const roundsRemaining = available
        ? 0
        : Math.max(1, (TRAP_COOLDOWN_ROUNDS + 1) - roundsSinceUse);

    return {
        code: targetRoom.code,
        available,
        roundsRemaining
    };
}

function emitTrapAbilityStatus(targetRoom, playerId) {
    io.to(playerId).emit('trap_status', trapAbilityStatusPayload(targetRoom, playerId));
}

// Round-based cooldown gate for the Innocent's "CHECK ROOM" ability (see
// 'check_room') — same shape as isTrapAbilityAvailable/markTrapAbilityUsed
// above, just keyed under its own game.markRoomLastUsedRound and using
// MARK_ROOM_COOLDOWN_ROUNDS, so it never shares (or clashes on) any other
// role's counters.
function isMarkRoomAbilityAvailable(game, playerId, currentRound) {
    if (!game.markRoomLastUsedRound) game.markRoomLastUsedRound = {};
    const lastUsed = game.markRoomLastUsedRound[playerId];
    if (lastUsed == null) return true;
    return (currentRound - lastUsed) >= MARK_ROOM_COOLDOWN_ROUNDS;
}

function markMarkRoomAbilityUsed(game, playerId, currentRound) {
    if (!game.markRoomLastUsedRound) game.markRoomLastUsedRound = {};
    game.markRoomLastUsedRound[playerId] = currentRound;
}

function markRoomAbilityStatusPayload(targetRoom, playerId) {
    const game = targetRoom?.game;
    const lastUsed = game?.markRoomLastUsedRound?.[playerId];
    if (lastUsed == null) {
        return { code: targetRoom.code, available: true, turnsRemaining: 0 };
    }

    const roundsSinceUse = game.round - lastUsed;
    const available = roundsSinceUse >= MARK_ROOM_COOLDOWN_ROUNDS;
    const turnsRemaining = available ? 0 : (MARK_ROOM_COOLDOWN_ROUNDS - roundsSinceUse);

    return {
        code: targetRoom.code,
        available,
        turnsRemaining
    };
}

function emitMarkRoomAbilityStatus(targetRoom, playerId) {
    io.to(playerId).emit('mark_room_status', markRoomAbilityStatusPayload(targetRoom, playerId));
}

// --- Trap DEBUFF: the actual "no actions/abilities" penalty a player eats for
// having walked into a trap (see triggerTrapIfPresent below). Deliberately
// keyed as game.trapDebuffRound[playerId] = the single round number the
// penalty applies to (rather than a boolean flag), so it self-expires the
// instant game.round moves past it — no separate cleanup/sweep pass needed
// anywhere else in the game loop. Set once, the instant the trap fires, to
// the round AFTER the one the trap was triggered in — the current round
// (trial included) plays out completely normally; the penalty covers the
// player's entire NEXT round, action phase and Court/Trial phase alike.
function isPlayerTrapDebuffed(game, playerId) {
    if (!game || !game.trapDebuffRound) return false;
    return game.trapDebuffRound[playerId] === game.round;
}

// Privately tells one player whether the trap debuff is currently active for
// them this round — sent at the start of both the action phase (round_start)
// and the Court/Trial phase (activateTrialVoting), same private/self-only
// treatment 'trap_status' above already gets, so the client can grey out
// actions/abilities proactively instead of only finding out via a rejection.
function emitTrapDebuffStatus(targetRoom, playerId) {
    io.to(playerId).emit('trap_debuff_status', {
        code: targetRoom.code,
        active: isPlayerTrapDebuffed(targetRoom.game, playerId)
    });
}

// Common rejection path for every action/ability handler below once a trap
// debuff is active — logs consistently and lets the client pop a single,
// unified explanation toast regardless of which action was attempted.
function rejectForTrapDebuff(targetRoom, socket, action) {
    console.log(`${action} REJECTED: ${socket.id} is trap-debuffed this round`);
    socket.emit('trap_debuff_blocked', { code: targetRoom.code, action });
}

// Fires whenever a player physically walks into a room that currently holds
// an unsprung trap (see 'set_trap') — via either 'select_room' or 'use_vent'.
// Consumes the trap immediately (one-time-use — removed from the room the
// instant it's triggered, so neither this player again nor anyone else
// walking in afterward triggers it a second time) and arms the trap debuff
// (see isPlayerTrapDebuffed) for this player's entire NEXT round: no
// investigating, no finding bodies, no active role abilities during that
// round's action phase, and none of the Court/Trial-phase abilities either.
function triggerTrapIfPresent(targetRoom, roomId, player) {
    const roomTraps = targetRoom.traps?.[roomId];
    if (!roomTraps || roomTraps.length === 0) return;

    // Consume the trap immediately — one-time use.
    delete targetRoom.traps[roomId];

    const game = targetRoom.game;
    let debuffRound = null;
    if (game) {
        if (!game.trapDebuffRound) game.trapDebuffRound = {};
        debuffRound = game.round + 1;
        game.trapDebuffRound[player.id] = debuffRound;
    }

    const roomInfo = findMansionRoomById(roomId);
    io.to(player.id).emit('trap_triggered', {
        code: targetRoom.code,
        roomId,
        roomName: roomInfo?.name || roomId,
        debuffRound
    });

    console.log(`trap_triggered: room=${targetRoom.code} player ${player.id} walked into a trap in "${roomInfo?.name || roomId}" — actions/abilities blocked for round ${debuffRound}`);
}

// Lets every client know whether the exit-code terminal is currently usable
// at all, without revealing WHERE any undiscovered body is — just the yes/no
// itself. Call this any time targetRoom.bodies (or a body's foundBy) changes:
// a kill lands, a hide/expose decision resolves, someone walks into/searches
// a room and finds a body, etc.
function broadcastExitStatus(targetRoom) {
    io.to(targetRoom.id).emit('exit_status', { code: targetRoom.code, sealed: hasUndiscoveredBody(targetRoom) });
}

// Room Restrictions: NO Actions in Holding Cell. True whenever `playerId`'s
// current server-authoritative location (game.playerLocations) is the
// Holding Cell — used as an explicit, defense-in-depth guard at the top of
// every turn-action handler (select_room, investigate_room, search_body,
// use_vent, kill_player, plant_joker_evidence), on top of whatever
// room-specific checks already happen to block it indirectly.
function isConfinedToHoldingCell(game, playerId) {
    return Boolean(game?.playerLocations && game.playerLocations[playerId] === 'f1_holding_cell');
}

// Finds whichever playerId currently holds a given role in this room (e.g.
// 'Killer', 'Accomplice') — used to privately notify one Killer-team member
// about something the other one did (see the Accomplice's trap notice to the
// Killer, and the Killer's accidental-evidence notice to the Accomplice).
// Returns undefined if nobody currently holds that role (e.g. mid-lobby, or
// a game without that optional role in play).
function findRoleHolderId(targetRoom, role) {
    if (!targetRoom.roles) return undefined;
    return Object.keys(targetRoom.roles).find(pid => targetRoom.roles[pid] === role);
}

function activeRoomOccupants(targetRoom, roomId) {
    // Player Visibility restriction: whoever is confined to the Holding Cell
    // must remain completely hidden/invisible to everyone else — no avatar,
    // no presence chip, nothing — whether the viewer is another active
    // player peeking the room, a spectator (see emitSpectatorRoomUpdates /
    // 'select_room's isObserverMode branch, which IS allowed to target
    // f1_holding_cell), or anyone else. The locked player's own client never
    // calls this for their own view either (see the dedicated round-long
    // Holding Cell view built client-side off `lockedInHoldingCell`), so an
    // unconditional empty list here is always correct, for every caller.
    if (roomId === 'f1_holding_cell') return [];

    const occupantIds = targetRoom.game?.roomOccupants?.[roomId] || [];
    return occupantIds
        .map(id => targetRoom.players.find(player => player.id === id))
        .filter(player => player && !player.isEliminated && !player.isObserver)
        .map(player => {
            const pos = getEntityPosition(targetRoom, roomId, player.id);
            return { id: player.id, nickname: player.nickname, character: player.character, x: pos.x, y: pos.y };
        });
}

// Bodies left exposed in `roomId` (isHidden: false) — these are meant to be
// stumbled onto just by walking in, unlike a hidden body which only ever
// surfaces via an explicit 'search_body'. Used both when a player freshly
// enters a room (see 'select_room' / 'use_vent' / 'resolve_kill') and when a
// spectator's peeked room updates live (see emitSpectatorRoomUpdates).
function exposedBodiesForRoom(targetRoom, roomId) {
    return (targetRoom.bodies || [])
        .filter(body => body.roomId === roomId && !body.isHidden)
        .map(body => {
            const pos = getEntityPosition(targetRoom, roomId, body.playerId || body.nickname);
            return { playerId: body.playerId || null, nickname: body.nickname, round: body.round, character: body.character || null, x: pos.x, y: pos.y };
        });
}

// Every piece of evidence the Joker has planted in this specific mansion room
// over the course of the match (see 'plant_joker_evidence'), oldest first.
// Returned to whoever searches the room — the server never says who planted
// it, only what was found.
function plantedEvidenceForRoom(targetRoom, roomId) {
    return (targetRoom.plantedEvidence?.[roomId] || []).map(({ id, text }) => ({ id, text }));
}

// Forensic Examiner — "Verify Evidence Authenticity": locates a single evidence
// entry by id, WITH its full authenticity flags (isPlanted / isFalse) intact —
// unlike plantedEvidenceForRoom/buildCluesBoard above, which deliberately strip
// those flags before anything reaches a client, since revealing them for free
// would make the Forensic Examiner's ability pointless. Searches both the
// still-in-room evidence (targetRoom.plantedEvidence, across every room, not
// just one) and the match-lifetime discoveredClues archive (see
// startNewRound), since a piece of evidence found in an earlier round has
// already been moved out of plantedEvidence but is still shown on the shared
// CLUES board and must still be checkable.
function findEvidenceEntryById(targetRoom, evidenceId) {
    const archived = (targetRoom.discoveredClues || []).find(entry => entry.id === evidenceId);
    if (archived) return archived;

    const roomBuckets = Object.values(targetRoom.plantedEvidence || {});
    for (const entries of roomBuckets) {
        const match = entries.find(entry => entry.id === evidenceId);
        if (match) return match;
    }
    return null;
}

// --- SHARED CLUES BOARD ------------------------------------------------------
// Every piece of Joker-planted evidence (see CHARACTER_EVIDENCE /
// 'plant_joker_evidence') that at least one player has actually walked into
// via 'investigate_room' graduates from "sitting in a room" to "on the case
// board" — visible to the whole room via the CLUES button, same audience as
// the trial vote menu and chat. This is intentionally separate from the
// digital code fragments: those stay Innocent-only exactly as before, since
// this board only ever reads from plantedEvidence, never evidenceLocations.
//
// `foundBy` lives directly on each clue entry inside targetRoom.plantedEvidence
// (rather than a separate lookup table) so it resets for free whenever
// plantedEvidence itself is reset/reshuffled (see startGame / resetRoomToLobby),
// and so a clue's discoverers are never duplicated: a player who searches the
// same room again later just finds the same entry, already carrying their name.
//
// Returns true if this call added at least one NEW discoverer (i.e. something
// actually changed and the board is worth re-broadcasting), false otherwise.
function registerClueDiscovery(targetRoom, roomId, player, round) {
    const entries = targetRoom.plantedEvidence?.[roomId];
    if (!entries || entries.length === 0) return false;

    let changed = false;
    entries.forEach(entry => {
        if (!entry.foundBy) entry.foundBy = [];
        const alreadyCredited = entry.foundBy.some(finder => finder.id === player.id);
        if (!alreadyCredited) {
            entry.foundBy.push({ id: player.id, nickname: player.nickname, round });
            changed = true;
        }
    });
    return changed;
}

// Flattens every discovered (foundBy.length > 0) clue across all rooms into a
// single list for the CLUES button. Clues nobody has found yet stay hidden —
// same principle as the mansion map itself, nothing is spoiled in advance.
// Ordered oldest-discovery-first via the clue's own id, which is a Date.now()-based
// token minted at plant time (see 'plant_joker_evidence').
//
// Evidence the Accomplice has doctored via 'accomplice_change_evidence' (see
// evidenceEntry.framedPlayerId there) is deliberately excluded here, on both
// loops below — that ability is meant to quietly mislead whoever personally
// walks in and investigates that specific room, NOT to broadcast a frame job
// to the whole group. It never gets disclosed on the shared board, and since
// buildFindingsSummary reuses this same function for the trial-phase recap,
// it never shows up there either. Genuine Joker-planted evidence has no
// framedPlayerId and is unaffected — that one's still meant to be public.
function buildCluesBoard(targetRoom) {
    const board = [];
    // Match-lifetime archive first (see discoveredClues / startNewRound) —
    // these have already been pruned out of plantedEvidence by a prior round
    // roll-over and would otherwise vanish from this board entirely.
    (targetRoom.discoveredClues || []).forEach(entry => {
        if (entry.framedPlayerId) return;
        board.push({
            id: entry.id,
            text: entry.text,
            description: entry.description || '',
            roomId: entry.roomId,
            roomName: entry.roomName,
            plantedRound: entry.round,
            foundBy: entry.foundBy.map(({ nickname, round }) => ({ nickname, round }))
        });
    });
    Object.values(targetRoom.plantedEvidence || {}).forEach(entries => {
        entries.forEach(entry => {
            if (entry.framedPlayerId) return;
            if (entry.foundBy && entry.foundBy.length > 0) {
                board.push({
                    id: entry.id,
                    text: entry.text,
                    description: entry.description || '',
                    roomId: entry.roomId,
                    roomName: entry.roomName,
                    plantedRound: entry.round,
                    foundBy: entry.foundBy.map(({ nickname, round }) => ({ nickname, round }))
                });
            }
        });
    });
    board.sort((a, b) => (a.id < b.id ? -1 : a.id > b.id ? 1 : 0));
    return board;
}

// Recap shown to everyone the instant the trial phase is announced (see
// startTrialPhase's TRIAL_ANNOUNCEMENT step) — "here's what the case file
// holds so far": every body someone has actually discovered (via walking
// into an exposed one, or an explicit 'search_body'; see
// creditExposedBodyDiscovery / 'search_body') and every clue the CLUES
// board already knows about (buildCluesBoard — already public the instant
// it's found, same rule applied here). A body nobody has found yet is
// deliberately left out of the `bodies` list itself — same reasoning as the
// exit-code block in 'submit_innocent_code': if it isn't in this recap, the
// case isn't closed. It's still surfaced as a bare `undiscoveredCount`,
// though, so the group at least knows someone is missing without leaking
// who or where.
function buildFindingsSummary(targetRoom) {
    // Match-lifetime archive (see discoveredBodies / startNewRound) plus
    // whatever's still sitting in targetRoom.bodies this round (found or
    // not) — a body found earlier this same round hasn't been archived yet,
    // so both sources are needed to avoid dropping it OR double-counting it.
    const allBodies = [...(targetRoom.discoveredBodies || []), ...(targetRoom.bodies || [])];
    const bodies = allBodies
        .filter(body => body.foundBy && body.foundBy.length > 0)
        .map(body => {
            const roomInfo = findMansionRoomById(body.roomId);
            return {
                bodyId: body.playerId || body.nickname,
                nickname: body.nickname,
                character: body.character || null,
                roomId: body.roomId,
                roomName: roomInfo?.name || body.roomId,
                // Deliberately no `round` here anymore — the trial recap shows
                // WHERE the body was found and WHO found it, not which round
                // the kill happened in (see foundBy below / dossier lookup
                // client-side via CHARACTERS[].name using `character`).
                foundBy: (body.foundBy || []).map(({ nickname }) => nickname),
                description: (body.character && BODY_DESCRIPTIONS[body.character]) || 'The scene offers no further detail.'
            };
        });
    // Deliberately just a count, never a name/room — a victim nobody has
    // found yet must stay completely unaccounted for, same reasoning as
    // hasUndiscoveredBody/'submit_innocent_code'. This only tells the group
    // "someone is missing", never who or where.
    const undiscoveredCount = allBodies.filter(body => !body.foundBy || body.foundBy.length === 0).length;
    return { bodies, clues: buildCluesBoard(targetRoom), undiscoveredCount };
}

function emitSpectatorRoomUpdates(targetRoom, roomId) {
    const views = targetRoom.game?.spectatorViews || {};
    const occupants = activeRoomOccupants(targetRoom, roomId);
    const bodies = exposedBodiesForRoom(targetRoom, roomId);
    Object.entries(views).forEach(([spectatorId, viewedRoomId]) => {
        if (viewedRoomId === roomId) {
            io.to(spectatorId).emit('spectator_room_update', { roomId, occupants, bodies });
        }
    });
}

function broadcastTrialPlayerList(targetRoom) {
    io.to(targetRoom.id).emit('trial_player_list', buildTrialRoster(targetRoom));
}

const adjectives = ['Secret', 'Dark', 'Shadowy', 'Silent', 'Mysterious', 'Hidden', 'Foggy', 'Coded', 'Bloody', 'Abandoned'];
const nouns = ['Lobby', 'Office', 'Room', 'Alley', 'Agency', 'Safehouse', 'Corner', 'Archive', 'Basement', 'HQ'];

// Roles that may only appear once in a match. The required base roles are
// always present (from the 5-player minimum up); the remaining special
// roles are only introduced once the lobby goes past seven players (8+).
const BASE_ROLES = ['Killer', 'Detective', 'Officer'];
const SPECIAL_ROLES = ['Joker', 'Accomplice', 'Forensic'];

// --- JOKER PLANTED EVIDENCE -------------------------------------------------
// Once per every 3 of their own turns, the Joker may plant a piece of physical
// evidence in whichever mansion room they're currently searching (see
// 'plant_joker_evidence'). Each clue is tied to the Joker's assigned character
// and deliberately echoes that character's hobbies/bio (see the CHARACTERS
// list above) so a sharp-eyed room searcher can piece together who planted it
// — without the server ever stating the character or player outright.
//
// Each character now maps to an ARRAY of clues rather than a single string, so
// a Joker who plants evidence multiple times over a match leaves behind a
// varied trail instead of repeating the same item. 'plant_joker_evidence'
// draws a random, non-repeating clue from this array per-player (see
// jokerCluePoolByPlayer below).
// Each clue is now a { name, description } pair rather than a single string:
// `name` is the short label shown the moment a room is searched (toast /
// room-peek list, same as before), while `description` is the longer flavor
// text that only surfaces in a clue's "dossier" once it's been added to the
// shared CLUES board (see buildCluesBoard / registerClueDiscovery).
const CHARACTER_EVIDENCE = {
    Creed: [
        { name: "Auction certificate", description: "An official proof of purchase for a digital artwork worth an astronomical sum, marked as paid in full." },
        { name: "Hardware crypto wallet", description: "A sleek, limited-edition titanium USB ledger with a laser-engraved serial number from an elite tech auction." },
        { name: "Vintage Swiss watch", description: "An elegant accessory with a fine leather strap, lightly stained by a drop of expensive red wine." },
        { name: "Watchmaker magnifier", description: "A high-precision jeweler's loupe used for inspecting intricate mechanical watch movements." },
        { name: "Crystal stopper", description: "A heavy stopper from a rare vintage wine bottle that still retains a deep, pungent aroma." }
    ],
    Karl: [
        { name: "Steel foil tip cover", description: "A protective tip guard for a competitive fencing foil, neatly engraved with the initials \"K.T.\"" },
        { name: "White fencing glove", description: "A pristine leather fencing glove bearing subtle chalk stains and a custom embroidered emblem." },
        { name: "Opera ticket stub", description: "A charred piece of a VIP ticket for a high-profile theater premiere in the front row." },
        { name: "Gold opera glasses", description: "A compact pair of vintage brass binoculars lined with mother-of-pearl." },
        { name: "Genealogy chart", description: "An excerpt from an antique book on royal dynasties with one family line circled in red marker." }
    ],
    Anthonio: [
        { name: "Rubber pool duck", description: "A child's rubber toy smeared with dark motor oil and industrial residue." },
        { name: "Duck feed container", description: "A small metal tin containing dry grain mix and tiny flecks of cigar tobacco." },
        { name: "Cigar butt with ash", description: "An unfinished imported cigar featuring a stamped gold band and fresh tobacco ash." },
        { name: "Engraved brass Zippo", description: "A heavy antique flip lighter smelling strongly of lighter fluid, with worn initials \"A.R.\" on the side." },
        { name: "Garbage contract", description: "A crumpled waste management agreement stamped with a smudged signature reading \"Rossi.\"" }
    ],
    James: [
        { name: "Guitar pick and string", description: "A black guitar pick with a stylized logo alongside a snapped steel guitar string." },
        { name: "Custom Explorer guitar strap", description: "A thick leather strap studded with iron spikes, smelling faintly of stage smoke." },
        { name: "Engine oil canister", description: "A metal oil dispenser from a high-powered V8 engine bearing clear fingerprints on its side." },
        { name: "Greasy shop rag", description: "A dark red cloth soaked in high-octane gasoline and heavy motor grease." },
        { name: "Rifle casing", description: "A spent large-caliber cartridge casing that clearly belonged to a rifled hunting carbine." }
    ],
    Cedric: [
        { name: "Exercise band", description: "A snapped rubber resistance band showing heavy wear from intense physical training." },
        { name: "Chalk dust pouch", description: "A small cloth bag filled with dry gymnastic chalk used to keep hands dry during heavy workouts." },
        { name: "Tattoo sketch", description: "A grim monochrome design drawn by hand on a crumpled piece of paper." },
        { name: "Drawing charcoal pencil", description: "A fine black graphite pencil worn down to the stub from shading intricate tattoo art." },
        { name: "Makeshift shank", description: "A heavy metal rod sharpened to a fine point with a handle wrapped tightly in black electrical tape." }
    ],
    Lidy: [
        { name: "Recipe card", description: "A card with a homemade pie recipe stained with deep red drops that look deceptively like blood." },
        { name: "Cherry pie tin", description: "A lightweight aluminum baking dish containing sticky sweet red syrup residue." },
        { name: "Skein of wool yarn", description: "A soft ball of thick yarn with a long steel knitting needle sticking out of it." },
        { name: "Stray wool thread", description: "A long strand of bright cherry-red yarn unraveled from a heavy handmade sweater." },
        { name: "Paperback novel", description: "A worn detective book with a bookmark placed right at the chapter titled \"The Killer Is...\"" }
    ],
    May: [
        { name: "Jar of glowing dust", description: "A glass vial containing phosphorescent powder and the cap of a dried wild forest mushroom." },
        { name: "Dried mushroom cap", description: "A brittle, bioluminescent forest mushroom that faintly glows in complete darkness." },
        { name: "Woven flower crown", description: "A dried wreath made of forest flowers with bits of green moss stuck between the stems." },
        { name: "Floral wire snips", description: "A tiny pair of rusty craft scissors used for trimming flower stems and vines." },
        { name: "Sparkling dust jar", description: "A tiny corked glass bottle filled with shimmering iridescent glitter and fine sand." }
    ],
    Gregory: [
        { name: "Suture thread", description: "A spool of fine surgical thread attached to a curved needle at its end." },
        { name: "Synthetic skin patch", description: "A rubbery practice pad showing a series of unnervingly neat, tight surgical stitches." },
        { name: "Surgical scalpel", description: "A vintage steel instrument with a razor-sharp blade bearing faint traces of synthetic skin." },
        { name: "Antique scalpel case", description: "A velvet-lined wooden box designed to hold a set of historical surgical instruments." },
        { name: "Anatomy page", description: "A torn page from a medical textbook analyzing vulnerable points along the carotid artery." }
    ],
    Onyx: [
        { name: "Locked black journal", description: "A pocket diary bound in a hard cover and secured with a miniature padlock." },
        { name: "Water-damaged note", description: "A handwritten note with words blurred by water reading \"...alone again in this dark room...\"" },
        { name: "Vinyl record sleeve fragment", description: "A piece of a rare vinyl record cover that smells faintly of rain and damp air." },
        { name: "Headphone jack adapter", description: "A small gold-plated audio converter attached to a tiny emo band emblem." },
        { name: "Damp umbrella cover", description: "A dripping wet nylon sleeve designed for a compact black umbrella." }
    ],
    Max: [
        { name: "Charcoal stick", description: "A stick of drawing charcoal that left dark dust on fingers from drawing chaotic wall patterns." },
        { name: "Smudged wall rubbing", description: "A piece of paper pressed against a hard surface, capturing chaotic charcoal spiral patterns." },
        { name: "Note to invisible visitors", description: "A crumpled piece of paper recording a strange handwritten dialogue with an unseen speaker." },
        { name: "Empty blister pack", description: "A completely empty foil pack that previously contained brightly colored prescription pills." },
        { name: "Pill bottle cap", description: "A safety cap from a prescription medicine bottle smeared with black charcoal dust." }
    ],
    Bea: [
        { name: "Katana sheath", description: "A leather sheath for a traditional Japanese sword that smells strongly of gun oil and polish." },
        { name: "Tsuka-ito ribbon", description: "A strip of tough, wax-treated black silk ribbon used for wrapping the hilt of a sword." },
        { name: "Revenge checklist", description: "A piece of paper with a list of names where two entries have been neatly crossed out." },
        { name: "Crossed-out name fragment", description: "A torn slip of paper showing a single target's name struck through with thick red ink." },
        { name: "Meditation blindfold", description: "A black silk headband that remains damp with sweat from a strenuous workout." }
    ],
    Moonka: [
        { name: "Literature club pin", description: "A metal lapel pin featuring a book emblem and an engraved poetic quote." },
        { name: "Metaphorical poem draft", description: "A sheet of paper featuring a deeply emotional poem about digital isolation and classical piano." },
        { name: "Piano sheet music", description: "A page from a notebook with a melancholy minor melody written down by hand." },
        { name: "USB drive", description: "A compact flash drive marked with a neat label reading \"script_v2.py\"." },
        { name: "Code printout", description: "A page of complex Python code with handwritten musical notes along the margins." }
    ]
};

// Short, flavor-rich (1-2 sentence) scene description shown for a victim's
// body once it's been discovered — surfaced in the Trial phase "Bodies" tab
// (see buildFindingsSummary) alongside where/when it was found. Keyed by
// CHARACTERS[].name, same key style as CHARACTER_EVIDENCE above.
const BODY_DESCRIPTIONS = {
    Creed: "Creed's body is frozen in an elegant pose on the floor, with a shattered expensive watch fallen from his pocket. A thin stream of blood trickles from the corner of his mouth, contrasting sharply with his tailored suit.",
    Karl: "Karl lies face down, his hand tightly clutching the tip of a fencing foil. Dark, dried blood stains his pristine white collar.",
    Anthonio: "Anthonio's massive frame lies heavily on its side, a luxury cigar still smoldering nearby. His eyes are wide open in quiet shock at his sudden demise.",
    James: "James rests against the wall as if thrown by a powerful blow, his black guitar pick lying a few feet away. Blood slowly oozes from a heavy wound on his chest.",
    Cedric: "Cedric's battle-hardened body lies in a defensive posture, though signs of struggle suggest a cowardly ambush. Fresh scrapes are visible on his tattooed arms.",
    Lidy: "Lidy's fragile body appears helpless, with scattered skeins of woolen yarn surrounding her. Her face is frozen in an expression of deep fear and betrayal.",
    May: "Little May lies motionless, surrounded by glowing powder spilled from a broken glass jar. Her woven flower wreath is knocked askew and partially trampled.",
    Gregory: "Dr. Chen fell flat on his back, surgical tools spilled out from his open medical bag. A precise, professional strike took his life in seconds.",
    Onyx: "Onyx's body lies still in the shadows, almost blending into them, with his locked black journal thrown to the side. The wet floor nearby bears marks of a brief struggle.",
    Max: "Max is frozen in an awkward position right by a wall covered in his chaotic charcoal sketches. Bright pills are scattered around from an open blister pack.",
    Bea: "Bea lies against the opposite wall, her hand still gripping the scabbard of a katana she never had the chance to draw. Pure fury at her unfulfilled revenge lingers in her glassy eyes.",
    Moonka: "Moonka lies motionless on the floor, clutching a small flash drive containing code. Her face looks strikingly peaceful, as if she managed to finish her final poem before the end."
};

// How many of the Joker's OWN turns must pass before they can plant another
// piece of evidence. Using it on turn N locks it until turn N+2 (i.e. the next
// one of their turns is blocked).
const JOKER_CLUE_COOLDOWN_TURNS = 2;

// How many of the Accomplice's OWN turns must pass before they can use "Change
// Evidence" again (see 'accomplice_change_evidence') — same shape/cooldown as
// the Joker's clue-planting cooldown above, just tracked under its own
// game.accompliceOwnTurnCount / game.accompliceEvidenceLastUsedOwnTurn keys so
// the two abilities never share (or clash on) the same counters.
const ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS = 3;

// How many whole game ROUNDS must pass before the Accomplice can "Set a
// Trap" (see 'set_trap') again — deliberately round-based rather than
// own-turn-based like the two cooldowns above, since a trap is meant to be a
// rarer, match-spanning tool rather than something reset every few of the
// Accomplice's own turns. Same available-when-roundsSinceUse>COOLDOWN shape
// as the Forensic Examiner's FORENSIC_SHARED_COOLDOWN_ROUNDS below — using it
// in round N locks it until round N+5 (i.e. the next 4 rounds are blocked).
const TRAP_COOLDOWN_ROUNDS = 4;

// Flavor text for the doctored evidence entry left behind by
// 'accomplice_change_evidence'. Rather than a generic "traced back to X"
// line, this now draws from the TARGET's own CHARACTER_EVIDENCE pool — the
// exact same 3-item pool the Joker plants from — so a framed player gets
// implicated by something that plausibly belongs to them (matching their
// character's hobbies/bio), not a vague catch-all sentence.
//
// Picks at RANDOM from the target's pool, never repeating one for that same
// target until all 3 have been used once (mirrors jokerCluePoolByPlayer).
// The pool is tracked on `game.accompliceFramePoolByTarget`, keyed by the
// FRAMED player's id (not the Accomplice's id) — several different pieces of
// evidence framing the same person should still cycle through all 3 clues
// rather than each roll being independent. Never reveals the Accomplice
// planted it — same anonymity rule every other planted-evidence path
// already follows.
function buildFramedEvidenceEntry(game, targetPlayer) {
    const evidencePool = CHARACTER_EVIDENCE[targetPlayer.character] || [{ name: 'Mysterious personal item', description: 'An unidentified item with no further clues.' }];

    if (!game.accompliceFramePoolByTarget) game.accompliceFramePoolByTarget = {};
    let pool = game.accompliceFramePoolByTarget[targetPlayer.id];
    if (!pool || pool.length === 0) {
        pool = shuffleArray(evidencePool.map((_, i) => i));
    }
    const clueIndex = pool.shift();
    game.accompliceFramePoolByTarget[targetPlayer.id] = pool;
    const clueData = evidencePool[clueIndex];

    return {
        text: clueData.name,
        description: clueData.description
    };
}

// How many ROUNDS must pass before the Detective's "Check Player's Last
// Location" ability (see 'detective_check_location') can be used again.
// Unlike the Joker's cooldown above (measured in the Joker's own turns),
// this is measured in whole game rounds, since the ability only fires once
// during the Court/Trial phase of a round — using it in round N locks it
// during round N+1's trial, and it re-arms for round N+2's trial.
const DETECTIVE_ABILITY_COOLDOWN_ROUNDS = 2;

// How many ROUNDS must pass before the Officer's "Lock in Holding Cell"
// ability (see 'officer_lock_player') can be used again. Same measurement
// style as DETECTIVE_ABILITY_COOLDOWN_ROUNDS above — using it during round N's
// trial locks it out through round N+1 and N+2's trials, and re-arms in time
// for round N+3's.
const OFFICER_ABILITY_COOLDOWN_ROUNDS = 3;

// Round-based cooldown for the Innocent's "CHECK ROOM" ability (see
// 'check_room' / Mark Room). Deliberately measured in whole rounds like
// Detective/Officer above, not the Joker/Accomplice's own-turn counters,
// since — unlike them — every Innocent in play shares this one team-wide
// piece of knowledge (a cleared room stays cleared for everyone), so gating
// it per-round rather than per-own-turn keeps a multi-Innocent team from
// carpet-marking the whole mansion in a single round.
const MARK_ROOM_COOLDOWN_ROUNDS = 2;

// --- MANSION LAYOUT: 2 floors of 10 rooms each. Used during a player's turn for
// the search phase (see 'select_room'). The structure (id + display name) must
// MATCH the frontend's MANSION_LAYOUT constant — the server is the source of truth
// for who is where (fog of war), and the client just draws the grid using these ids.
const MANSION = {
    0: [
        // One single large room spanning the whole basement floor — purely
        // atmospheric (it's the room voted-out players are narratively taken
        // to, see the trial/execution flow), but freely walkable like any
        // other room: no special restriction like f1_holding_cell below.
        { id: 'b_torture', name: 'Torture Room' }
    ],
    1: [
        { id: 'f1_hall', name: 'Grand Hall' },
        { id: 'f1_library', name: 'Library' },
        { id: 'f1_kitchen', name: 'Kitchen' },
        { id: 'f1_dining', name: 'Dining Room' },
        { id: 'f1_study', name: 'Study' },
        { id: 'f1_conservatory', name: 'Conservatory' },
        { id: 'f1_cellar', name: 'Wine Cellar' },
        { id: 'f1_ballroom', name: 'Ballroom' },
        { id: 'f1_armory', name: 'Armory' },
        { id: 'f1_garage', name: 'Garage' },
        // Officer's holding cell for detained suspects — bottom-right corner of floor 1.
        { id: 'f1_holding_cell', name: 'Holding Cell' }
    ],
    2: [
        { id: 'f2_master', name: 'Master Bedroom' },
        { id: 'f2_guest', name: 'Guest Room' },
        { id: 'f2_bath', name: 'Bathroom' },
        { id: 'f2_office', name: 'Private Office' },
        { id: 'f2_attic', name: 'Attic' },
        { id: 'f2_gallery', name: 'Portrait Gallery' },
        { id: 'f2_terrace', name: 'Terrace' },
        { id: 'f2_nursery', name: 'Nursery' },
        { id: 'f2_archive', name: 'Archive' },
        { id: 'f2_observatory', name: 'Observatory' }
    ]
};

// --- VENTILATION SYSTEM: Killer-only shortcut passages between six mansion
// rooms. Each key is a ONE-WAY hop (source -> destination); a pair of rooms
// that can be vented between in both directions needs both directions listed
// explicitly here — nothing is inferred to be bidirectional automatically.
// Must stay in sync with the frontend's VENTS constant, same reasoning as
// MANSION above (server is the source of truth, client just draws it).
const VENTS = {
    f1_hall: 'f2_master',
    f2_master: 'f1_hall',
    f1_kitchen: 'f1_armory',
    f1_armory: 'f1_kitchen',
    f1_cellar: 'f2_attic',
    f2_attic: 'f1_cellar'
};

// Looks up a mansion room by id across both floors, returns the room with a floor field added.
function findMansionRoomById(roomId) {
    for (const floorKey of Object.keys(MANSION)) {
        const found = MANSION[floorKey].find(r => r.id === roomId);
        if (found) return { ...found, floor: Number(floorKey) };
    }
    return null;
}

// --- DIGITAL CODE / EVIDENCE FRAGMENTS -------------------------------------
// The Innocents collectively "know" a numeric digital code that the searching
// players are trying to piece together over the course of the match. Its
// length scales with how many Innocents are in play (more Innocents = more
// fragments to hide = a longer code), and each digit is planted as a single
// fragment in its own random mansion room. This assignment happens exactly
// ONCE, right after roles are assigned, and is never touched again — the
// fragment-to-room mapping must stay identical for the rest of the match,
// across every round, so repeated searches of the same room keep finding the
// same (or no) fragment.
const MIN_CODE_LENGTH = 4;
const MAX_CODE_LENGTH = 10;

function codeLengthForInnocents(innocentCount) {
    // 1 Innocent -> 4 digits, each additional Innocent -> +1 digit, capped at 10.
    const scaled = MIN_CODE_LENGTH + Math.max(0, innocentCount - 1);
    return Math.min(MAX_CODE_LENGTH, Math.max(MIN_CODE_LENGTH, scaled));
}

function generateNumericCode(length) {
    let code = '';
    for (let i = 0; i < length; i++) {
        code += Math.floor(Math.random() * 10).toString();
    }
    return code;
}

// Returns every searchable mansion room id (both floors), excluding the
// holding cell — that room is spectator-only and never holds a fragment.
function allSearchableRoomIds() {
    return Object.values(MANSION)
        .flat()
        .map(room => room.id)
        .filter(id => id !== 'f1_holding_cell');
}

// Generates the match's digital code and statically plants one digit per
// fragment in a random, distinct room. Stored on the room object (not the
// per-round `game` state) so it persists unchanged even as `game` gets
// rebuilt round to round via startNewRound.
function assignEvidenceLocations(targetRoom) {
    const innocentCount = Object.values(targetRoom.roles || {}).filter(role => role === 'Innocent').length;
    const codeLength = codeLengthForInnocents(innocentCount);
    const digitalCode = generateNumericCode(codeLength);

    const shuffledRoomIds = shuffleArray(allSearchableRoomIds());
    const chosenRoomIds = shuffledRoomIds.slice(0, codeLength);

    // roomId -> { digit, position } fragment info, fixed for the whole match.
    // `position` is the digit's 1-based place within the final code (e.g. "1/4"),
    // used only for the "Fragment i/N" display — NOT the order rooms get searched in.
    const evidenceLocations = {};
    chosenRoomIds.forEach((roomId, index) => {
        evidenceLocations[roomId] = { digit: digitalCode[index], position: index + 1 };
    });

    targetRoom.digitalCode = digitalCode;
    targetRoom.evidenceLocations = evidenceLocations;

    // Mark Room: roomId -> { round } for every searchable room an Innocent has
    // personally confirmed (via 'check_room') holds no code fragment. Fixed
    // for the whole match just like evidenceLocations above (a room's
    // fragment status never changes mid-match), and reset alongside it in
    // resetRoomToLobby. Populated lazily inside 'check_room' — never here,
    // since nothing has been checked yet.
    targetRoom.innocentClearedRooms = {};

    console.log(`Digital code assigned for room ${targetRoom.code}: ${digitalCode} (${innocentCount} innocent(s), ${codeLength} digits) ->`, evidenceLocations);
}

function generateUniqueRoomCode() {
    const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
    let code;
    let isDuplicate = true;

    while (isDuplicate) {
        code = '';
        for (let i = 0; i < 8; i++) {
            code += chars.charAt(Math.floor(Math.random() * chars.length));
        }
        const codeExists = Object.values(rooms).some(room => room.code === code);
        if (!codeExists) {
            isDuplicate = false;
        }
    }
    return code;
}

function generateUniqueRoomName() {
    let name;
    let isDuplicate = true;

    while (isDuplicate) {
        const adj = adjectives[Math.floor(Math.random() * adjectives.length)];
        const noun = nouns[Math.floor(Math.random() * nouns.length)];
        const num = Math.floor(100 + Math.random() * 900);
        name = `${adj} ${noun} #${num}`;

        const nameExists = Object.values(rooms).some(room => room.name === name);
        if (!nameExists) {
            isDuplicate = false;
        }
    }
    return name;
}

// Logging helper for room state — makes it easy to see what's actually in
// rooms[...] right before broadcasting.
function logRoomState(label, room) {
    if (!room) {
        console.log(`[${label}] room is undefined`);
        return;
    }
    console.log(
        `[${label}] room=${room.code} status=${room.status} host=${room.hostId} players=`,
        room.players.map(p => ({ id: p.id, nick: p.nickname, char: p.character, ready: p.isReady }))
    );
}

// List of public rooms available to join. Rooms in 'preparing' status (locked by
// the host, including a fully started game) are deliberately EXCLUDED from the
// list — they can't be joined, and they disappear entirely from the public lobby.
function publicRoomsList() {
    return Object.values(rooms)
        .filter(r => r.type === 'public' && r.status === 'open')
        .map(r => ({
            id: r.id,
            code: r.code,
            name: r.name,
            playersCount: r.players.length,
            minPlayers: MIN_PLAYERS,
            maxPlayers: MAX_PLAYERS
        }));
}

function cancelCountdown(targetRoom) {
    if (roomTimers[targetRoom.id]) {
        clearInterval(roomTimers[targetRoom.id]);
        delete roomTimers[targetRoom.id];
        io.to(targetRoom.id).emit('countdown_cancel', { code: targetRoom.code });
        console.log(`Countdown CANCELLED for room ${targetRoom.code}`);
    }
}

function startCountdown(targetRoom) {
    if (roomTimers[targetRoom.id]) return; // already running

    let remaining = 5;
    console.log(`Countdown STARTED for room ${targetRoom.code}`);
    io.to(targetRoom.id).emit('countdown_tick', { code: targetRoom.code, remaining });

    roomTimers[targetRoom.id] = setInterval(() => {
        remaining -= 1;

        if (remaining < 0) {
            clearInterval(roomTimers[targetRoom.id]);
            delete roomTimers[targetRoom.id];
            return;
        }

        io.to(targetRoom.id).emit('countdown_tick', { code: targetRoom.code, remaining });

        if (remaining === 0) {
            console.log(`Countdown finished for room ${targetRoom.code} — GAME START`);
            io.to(targetRoom.id).emit('game_start', { code: targetRoom.code });
            clearInterval(roomTimers[targetRoom.id]);
            delete roomTimers[targetRoom.id];

            // Reset load/intro/skip/role state before entering the game again
            targetRoom.loadedPlayers = [];
            targetRoom.skipVotes = [];
            targetRoom.introStarted = false;
            targetRoom.roles = {};
            targetRoom.roleRevealConfirmed = [];
            targetRoom.game = null;

            // Just in case, clear a leftover load-wait timer from a previous run
            if (roomLoadTimers[targetRoom.id]) {
                clearTimeout(roomLoadTimers[targetRoom.id]);
                delete roomLoadTimers[targetRoom.id];
            }
            const roleTimerKey = `${targetRoom.id}_role`;
            if (roomLoadTimers[roleTimerKey]) {
                clearTimeout(roomLoadTimers[roleTimerKey]);
                delete roomLoadTimers[roleTimerKey];
            }
            clearTurnTimer(targetRoom.id);
        }
    }, 1000);
}

// Shuffles an array (Fisher–Yates) without mutating the original
function shuffleArray(arr) {
    const a = [...arr];
    for (let i = a.length - 1; i > 0; i--) {
        const j = Math.floor(Math.random() * (i + 1));
        [a[i], a[j]] = [a[j], a[i]];
    }
    return a;
}

// Shuffles the player turn order so it's guaranteed to differ from the previous
// round's order (an identical sequence still counts as "the same" order, even
// across a round separated by the trial phase). With 0-1 players, or if a
// reasonable number of attempts fails to produce a different order (e.g. the
// player list changed so much there's nothing meaningful to compare), returns
// whatever was generated.
function shuffleTurnOrder(playerIds, previousOrder) {
    if (playerIds.length <= 1) return [...playerIds];

    let order;
    let attempts = 0;
    do {
        order = shuffleArray(playerIds);
        attempts++;
    } while (
        previousOrder &&
        previousOrder.length === order.length &&
        order.every((id, i) => id === previousOrder[i]) &&
        attempts < 20
    );
    return order;
}

// --- GAME LOOP: rounds, turn order, 30s turn timer, trial phase placeholder ---

// Starts the turn of whoever is next in the order: broadcasts 'turn_start' with
// endsAt (the absolute turn end time) and sets a server-side watchdog for
// TURN_DURATION_MS that advances the queue on its own if the player doesn't end
// their turn manually (or by searching a room) in time. There is no penalty
// for a previous turn timing out — every turn here is a normal, full-length
// turn (see handleTurnTimeout for what happens when THIS turn's timer expires).
function startPlayerTurn(targetRoom) {
    const game = targetRoom.game;
    if (!game) return;

    const currentPlayerId = game.turnOrder[game.currentTurnIndex];

    // --- HOLDING CELL: AUTO-SKIP, NO TIMER ------------------------------
    // Whoever the Officer confined to the Holding Cell this round (see
    // startNewRound / game.lockedInHoldingCellPlayerId) is skipped the
    // instant their turn comes up — no room picker, no actions, and
    // deliberately NO countdown timer ever shown to them (or anyone else)
    // for this "turn". `turn_start` still fires so every client's queue
    // position stays in sync, but with endsAt: null / duration: 0 and a
    // `skipped` flag so the client knows not to render a live timer for it.
    // The locked player's own screen doesn't even branch on turn_start at
    // all — they're shown a dedicated, persistent Holding Cell view for the
    // WHOLE round instead (driven client-side by the public
    // `lockedInHoldingCell` field sent on 'round_start' / 'phase_state').
    if (game.lockedInHoldingCellPlayerId === currentPlayerId) {
        game.turnEndsAt = null;
        game.turnToken = (game.turnToken || 0) + 1;
        const myToken = game.turnToken;

        io.to(targetRoom.id).emit('turn_start', {
            code: targetRoom.code,
            round: game.round,
            playerId: currentPlayerId,
            turnIndex: game.currentTurnIndex,
            totalTurns: game.turnOrder.length,
            duration: 0,
            endsAt: null,
            skipped: true,
            skipReason: 'holding_cell'
        });

        console.log(`Room ${targetRoom.code}: turn ${game.currentTurnIndex + 1}/${game.turnOrder.length} — player ${currentPlayerId} AUTO-SKIPPED (locked in Holding Cell), round ${game.round}`);

        clearTurnTimer(targetRoom.id);
        roomTurnTimers[targetRoom.id] = setTimeout(() => {
            if (game.turnToken !== myToken) return; // the turn already advanced some other way
            advanceTurn(targetRoom);
        }, HOLDING_CELL_SKIP_DELAY_MS);
        return;
    }

    const turnDurationMs = TURN_DURATION_MS;
    const endsAt = Date.now() + turnDurationMs;

    game.turnEndsAt = endsAt;
    // A token so an old setTimeout (e.g. from a turn ended early via end_turn, or
    // shortened after a room search) can't accidentally advance a different,
    // newer turn.
    game.turnToken = (game.turnToken || 0) + 1;
    const myToken = game.turnToken;

    // Advance the Joker's personal turn counter and privately let them know
    // whether 'plant_joker_evidence' is off cooldown for this turn. Sent only
    // to their own socket — every other role stays completely unaware this
    // even exists.
    if (targetRoom.roles && targetRoom.roles[currentPlayerId] === 'Joker') {
        if (!game.jokerOwnTurnCount) game.jokerOwnTurnCount = {};
        if (!game.jokerClueLastUsedOwnTurn) game.jokerClueLastUsedOwnTurn = {};
        game.jokerOwnTurnCount[currentPlayerId] = (game.jokerOwnTurnCount[currentPlayerId] || 0) + 1;
        const ownTurnCount = game.jokerOwnTurnCount[currentPlayerId];
        const lastUsedOwnTurn = game.jokerClueLastUsedOwnTurn[currentPlayerId];
        const turnsSinceUse = lastUsedOwnTurn == null ? null : ownTurnCount - lastUsedOwnTurn;
        const available = lastUsedOwnTurn == null || turnsSinceUse >= JOKER_CLUE_COOLDOWN_TURNS;
        io.to(currentPlayerId).emit('joker_evidence_status', {
            code: targetRoom.code,
            available,
            turnsRemaining: available ? 0 : (JOKER_CLUE_COOLDOWN_TURNS - turnsSinceUse)
        });
    }

    // Same per-own-turn cooldown bookkeeping as the Joker block above, but for
    // the Accomplice's "Change Evidence" ability (see
    // 'accomplice_change_evidence' / ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS). Sent
    // only to the Accomplice's own socket — every other role never learns this
    // status exists.
    if (targetRoom.roles && targetRoom.roles[currentPlayerId] === 'Accomplice') {
        if (!game.accompliceOwnTurnCount) game.accompliceOwnTurnCount = {};
        if (!game.accompliceEvidenceLastUsedOwnTurn) game.accompliceEvidenceLastUsedOwnTurn = {};
        game.accompliceOwnTurnCount[currentPlayerId] = (game.accompliceOwnTurnCount[currentPlayerId] || 0) + 1;
        const accompliceOwnTurnCount = game.accompliceOwnTurnCount[currentPlayerId];
        const accompliceLastUsedOwnTurn = game.accompliceEvidenceLastUsedOwnTurn[currentPlayerId];
        const accompliceTurnsSinceUse = accompliceLastUsedOwnTurn == null ? null : accompliceOwnTurnCount - accompliceLastUsedOwnTurn;
        const accompliceAvailable = accompliceLastUsedOwnTurn == null || accompliceTurnsSinceUse >= ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS;
        io.to(currentPlayerId).emit('accomplice_evidence_status', {
            code: targetRoom.code,
            available: accompliceAvailable,
            turnsRemaining: accompliceAvailable ? 0 : (ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS - accompliceTurnsSinceUse)
        });
        // "Set a Trap" (see 'set_trap') is a separate ability from "Change
        // Evidence" above with its own, round-based cooldown (TRAP_COOLDOWN_ROUNDS)
        // rather than an own-turn one — refreshed here too so the button under
        // the map reflects the current cooldown the instant the Accomplice's
        // turn starts.
        emitTrapAbilityStatus(targetRoom, currentPlayerId);
    }

    // Forensic Examiner ability status is shared across both "Verify Evidence"
    // and "Examine Body" — the same round-based cooldown tracker governs both.
    if (targetRoom.roles && targetRoom.roles[currentPlayerId] === 'Forensic') {
        emitForensicAbilityStatus(targetRoom, currentPlayerId);
    }

    // Innocent's "CHECK ROOM" (Mark Room) ability status — refreshed here so
    // the button reflects the current round-based cooldown the instant the
    // Innocent's own turn starts, same treatment as the Forensic/Accomplice
    // blocks above.
    if (targetRoom.roles && targetRoom.roles[currentPlayerId] === 'Innocent') {
        emitMarkRoomAbilityStatus(targetRoom, currentPlayerId);
    }

    io.to(targetRoom.id).emit('turn_start', {
        code: targetRoom.code,
        round: game.round,
        playerId: currentPlayerId,
        turnIndex: game.currentTurnIndex,
        totalTurns: game.turnOrder.length,
        duration: turnDurationMs / 1000,
        endsAt
    });

    console.log(`Room ${targetRoom.code}: turn ${game.currentTurnIndex + 1}/${game.turnOrder.length} — player ${currentPlayerId}, round ${game.round}`);

    clearTurnTimer(targetRoom.id);
    roomTurnTimers[targetRoom.id] = setTimeout(() => {
        if (game.turnToken !== myToken) return; // the turn already advanced some other way
        handleTurnTimeout(targetRoom, currentPlayerId);
    }, turnDurationMs);
}

// Called when a player's turn runs out the clock without them ending it
// manually. Two cases, neither of which carries any penalty into future
// turns:
//   - They never picked a room all turn (still on the map) — true AFK. They
//     get teleported into a random searchable room just so they're physically
//     placed somewhere for the rest of the match's bookkeeping (detective
//     checks, body/clue discovery, etc.), and their client is shown that
//     room's interior immediately via the same 'room_entered' payload a real
//     'select_room' would send — including whoever else is already there.
//   - They DID pick a room this turn, just never ended it manually — the turn
//     simply ends where they already are, nothing to change.
// Either way, turn control just passes to the next player as usual.
function handleTurnTimeout(targetRoom, playerId) {
    console.log(`Room ${targetRoom.code}: turn TIMED OUT for player ${playerId}`);
    const game = targetRoom.game;
    if (!game) return;

    const player = targetRoom.players.find(p => p.id === playerId);
    if (player && !player.isEliminated && !player.isObserver) {
        if (!game.playerLocations) game.playerLocations = {};
        if (!game.roomOccupants) game.roomOccupants = {};

        if (!game.playerLocations[playerId]) {
            const candidateRoomIds = allSearchableRoomIds();
            const randomRoomId = candidateRoomIds[Math.floor(Math.random() * candidateRoomIds.length)];
            const roomInfo = findMansionRoomById(randomRoomId);
            const occupantIds = game.roomOccupants[randomRoomId] || [];
            const occupants = activeRoomOccupants(targetRoom, randomRoomId).filter(o => o.nickname !== player.nickname);

            game.playerLocations[playerId] = randomRoomId;
            game.roomOccupants[randomRoomId] = occupantIds.includes(playerId) ? occupantIds : [...occupantIds, playerId];

            // Same "stumbled onto it" credit a real 'select_room' would give —
            // an exposed body sitting in the room they got dropped into is
            // discovered the same way.
            if (creditExposedBodyDiscovery(targetRoom, randomRoomId, player, game.round)) {
                broadcastExitStatus(targetRoom);
            }
            emitSpectatorRoomUpdates(targetRoom, randomRoomId);

            io.to(playerId).emit('room_entered', {
                roomId: roomInfo.id,
                roomName: roomInfo.name,
                floor: roomInfo.floor,
                occupants,
                bodies: exposedBodiesForRoom(targetRoom, randomRoomId),
                playerLocations: { ...game.playerLocations },
                inspectMs: ROOM_INSPECT_MS
            });

            console.log(`Room ${targetRoom.code}: player ${playerId} was AFK all turn — auto-placed in "${roomInfo.name}"`);
        } else {
            console.log(`Room ${targetRoom.code}: player ${playerId} ran out of time inside a room — turn ends where they are`);
        }
    }

    advanceTurn(targetRoom);
}

// Advances the queue to the next player; if the round's turns are done, move to trial.
function advanceTurn(targetRoom) {
    clearTurnTimer(targetRoom.id);
    const game = targetRoom.game;
    if (!game) return;

    // A Killer whose turn ends (timeout or manual 'end_turn') without ever
    // resolving what happens to their victim's body defaults to leaving it
    // exposed — better than silently losing track of it.
    if (game.pendingKillDecision) {
        const pending = game.pendingKillDecision;
        if (!targetRoom.bodies) targetRoom.bodies = [];
        targetRoom.bodies.push({
            playerId: pending.targetId,
            nickname: pending.targetNickname,
            character: pending.targetCharacter || null,
            roomId: pending.roomId,
            round: game.round,
            isHidden: false,
            foundBy: []
        });
        console.log(`Room ${targetRoom.code}: post-kill decision auto-resolved as "expose" (turn ended) for ${pending.targetNickname}`);
        emitSpectatorRoomUpdates(targetRoom, pending.roomId);
        broadcastExitStatus(targetRoom);
        game.pendingKillDecision = null;
    }

    game.currentTurnIndex += 1;

    // Skip over anyone eliminated/observing since the turn order was built for
    // this round (e.g. a Killer's victim, or an executed player) — they no
    // longer get a turn of their own.
    while (
        game.currentTurnIndex < game.turnOrder.length
        && (() => {
            const nextPlayer = targetRoom.players.find(p => p.id === game.turnOrder[game.currentTurnIndex]);
            return !nextPlayer || nextPlayer.isEliminated || nextPlayer.isObserver;
        })()
    ) {
        game.currentTurnIndex += 1;
    }

    if (game.currentTurnIndex >= game.turnOrder.length) {
        startTrialPhase(targetRoom);
    } else {
        startPlayerTurn(targetRoom);
    }
}

// Trial phase: voting on who to execute. Waits TRIAL_DURATION_MS then starts the
// next round with a freshly shuffled turn order. Also builds the `candidates`
// list (nickname + character + running vote tally per active player) so the
// voting UI can render one card per player without any extra lookups.
function activateTrialVoting(targetRoom, game, source = 'timeline') {
    try {
        if (targetRoom.game !== game || game.timelinePhase === 'TRIAL_VOTING') return;

        // Rebuild these at the instant voting opens.  This makes the phase-state
        // response, the trial_start event, and reconnecting clients agree on the
        // same current roster even if someone left during the cinematic.
        const eligibleVoterIds = getActivePlayerIds(targetRoom);
        const voteBreakdown = buildTrialVoteBreakdown(game.trial, eligibleVoterIds);
        const candidates = buildTrialCandidates(targetRoom, eligibleVoterIds, voteBreakdown.playerVotes);
        const players = buildTrialRoster(targetRoom);

        clearTrialTransitionTimers(targetRoom.id);
        clearTurnTimer(targetRoom.id);
        clearTrialTickTimer(targetRoom.id);
        game.phase = 'trial';
        game.timelinePhase = 'TRIAL_VOTING';
        game.phaseStartTime = Date.now();
        game.trial = {
            ...(game.trial || {}),
            status: 'voting',
            eligibleVoterIds,
            candidates,
            confirmedVoterIds: [],
            endsAt: Date.now() + TRIAL_DURATION_MS
        };

        broadcastPhaseState(targetRoom);
        io.to(targetRoom.id).emit('trial_start', {
            code: targetRoom.code, round: game.round, duration: TRIAL_DURATION_MS / 1000,
            endsAt: game.trial.endsAt, eligibleVoterIds, candidates, players
        });
        io.to(targetRoom.id).emit('trial_roster_update', { code: targetRoom.code, players });
        broadcastTrialPlayerList(targetRoom);

        // Privately tell the Detective (and only the Detective) whether their
        // "Check Player's Last Location" ability is off cooldown for this
        // Court/Trial phase. Sent only to their own socket — every other role
        // stays completely unaware this ability even exists, same treatment
        // as the Joker's 'joker_evidence_status' above.
        if (!game.detectiveAbilityLastUsedRound) game.detectiveAbilityLastUsedRound = {};
        targetRoom.players.forEach(p => {
            if (!targetRoom.roles || targetRoom.roles[p.id] !== 'Detective') return;
            const lastUsedRound = game.detectiveAbilityLastUsedRound[p.id];
            const roundsSinceUse = lastUsedRound == null ? null : game.round - lastUsedRound;
            const available = lastUsedRound == null || roundsSinceUse >= DETECTIVE_ABILITY_COOLDOWN_ROUNDS;
            io.to(p.id).emit('detective_ability_status', {
                code: targetRoom.code,
                available,
                turnsRemaining: available ? 0 : (DETECTIVE_ABILITY_COOLDOWN_ROUNDS - roundsSinceUse)
            });
        });

        // Same private, role-only treatment for the Officer's "Lock in
        // Holding Cell" ability — off-cooldown status sent only to their own
        // socket at the start of every Court/Trial phase.
        if (!game.officerAbilityLastUsedRound) game.officerAbilityLastUsedRound = {};
        targetRoom.players.forEach(p => {
            if (!targetRoom.roles || targetRoom.roles[p.id] !== 'Officer') return;
            const lastUsedRound = game.officerAbilityLastUsedRound[p.id];
            const roundsSinceUse = lastUsedRound == null ? null : game.round - lastUsedRound;
            const available = lastUsedRound == null || roundsSinceUse >= OFFICER_ABILITY_COOLDOWN_ROUNDS;
            io.to(p.id).emit('officer_ability_status', {
                code: targetRoom.code,
                available,
                turnsRemaining: available ? 0 : (OFFICER_ABILITY_COOLDOWN_ROUNDS - roundsSinceUse)
            });
        });

        // Re-confirm the trap debuff (see isPlayerTrapDebuffed) to every
        // active player the instant the Court/Trial phase opens — same round
        // number as the action phase they just left, so anyone debuffed for
        // this round stays locked out of Court abilities too.
        targetRoom.players.forEach(p => emitTrapDebuffStatus(targetRoom, p.id));

        emitTrialTimer(targetRoom);
        trialTickTimers[targetRoom.id] = setInterval(() => {
            const trial = targetRoom.game?.trial;
            if (!trial || trial.status !== 'voting') return clearTrialTickTimer(targetRoom.id);
            emitTrialTimer(targetRoom);
        }, 1000);
        roomTurnTimers[targetRoom.id] = setTimeout(() => resolveTrialPhase(targetRoom), TRIAL_DURATION_MS);
    } catch (error) {
        console.error(`Room ${targetRoom.code}: failed to activate trial voting (${source})`, error);
    }
}

function startTrialPhase(targetRoom) {
    const game = targetRoom.game;
    if (!game) return;

    // A new transition invalidates every prior transition timeout first, so an
    // old callback can never overwrite this run's state.
    clearTurnTimer(targetRoom.id);
    clearTrialTickTimer(targetRoom.id);
    clearTrialTransitionTimers(targetRoom.id);

    try {
        const eligibleVoterIds = getActivePlayerIds(targetRoom);
        game.phase = 'transition';
        game.timelinePhase = 'TRANSITION_TO_TRIAL';
        game.phaseStartTime = Date.now();
        game.turnEndsAt = null;
        // Freeze this round's movement fog into a stable snapshot the instant
        // the Court/Trial phase begins — this is exactly "the room where each
        // player ended their previous turn" that the Detective's ability (see
        // 'detective_check_location') answers against. game.playerLocations
        // itself gets wiped clean at the start of the NEXT round (startNewRound),
        // so this copy is what keeps the answer available/stable for the
        // duration of this trial.
        game.lastKnownEndRoom = { ...(game.playerLocations || {}) };
        game.trial = {
            status: 'pending', votes: {}, confirmedVoterIds: [], voteSummary: {}, skipVotes: 0,
            candidates: buildTrialCandidates(targetRoom, eligibleVoterIds, {}),
            eligibleVoterIds, endsAt: null, result: null
        };
        broadcastPhaseState(targetRoom);

        phaseTimelineTimers[targetRoom.id] = setTimeout(() => {
            delete phaseTimelineTimers[targetRoom.id];
            try {
                if (targetRoom.game !== game || game.timelinePhase !== 'TRANSITION_TO_TRIAL') return;
                game.timelinePhase = 'TRIAL_ANNOUNCEMENT';
                game.phaseStartTime = Date.now();
                broadcastPhaseState(targetRoom);
                phaseTimelineTimers[targetRoom.id] = setTimeout(() => {
                    delete phaseTimelineTimers[targetRoom.id];
                    try {
                        if (targetRoom.game !== game || game.timelinePhase !== 'TRIAL_ANNOUNCEMENT') return;
                        activateTrialVoting(targetRoom, game);
                    } catch (error) {
                        console.error(`Room ${targetRoom.code}: trial announcement callback failed`, error);
                        activateTrialVoting(targetRoom, game, 'announcement recovery');
                    }
                }, TRIAL_ANNOUNCEMENT_MS);
            } catch (error) {
                console.error(`Room ${targetRoom.code}: trial blackout callback failed`, error);
                activateTrialVoting(targetRoom, game, 'blackout recovery');
            }
        }, TRIAL_BLACKOUT_MS);

        // This is deliberately longer than the ~7.5s timeline (1.5s blackout +
        // 6s announcement). It is an independent escape hatch for any
        // unexpected transition failure.
        trialTransitionSafetyTimers[targetRoom.id] = setTimeout(() => {
            delete trialTransitionSafetyTimers[targetRoom.id];
            if (targetRoom.game === game && ['TRANSITION_TO_TRIAL', 'TRIAL_ANNOUNCEMENT'].includes(game.timelinePhase)) {
                console.warn(`Room ${targetRoom.code}: forcing TRIAL_VOTING after stalled transition`);
                activateTrialVoting(targetRoom, game, '9s safety fallback');
            }
        }, 9000);
    } catch (error) {
        console.error(`Room ${targetRoom.code}: failed to start trial transition`, error);
        activateTrialVoting(targetRoom, game, 'start recovery');
    }
}

function resolveTrialPhase(targetRoom) {
    const game = targetRoom.game;
    if (!game || game.phase !== 'trial') return;

    clearTurnTimer(targetRoom.id);
    clearTrialTickTimer(targetRoom.id);
    clearTrialTransitionTimers(targetRoom.id);
    const trial = game.trial || {};
    const eligibleVoterIds = trial.eligibleVoterIds || getActivePlayerIds(targetRoom);
    const voteBreakdown = buildTrialVoteBreakdown(trial, eligibleVoterIds);
    const voteSummary = voteBreakdown.playerVotes;
    const candidates = buildTrialCandidates(targetRoom, eligibleVoterIds, voteSummary);
    const orderedResults = Object.entries(voteSummary)
        .filter(([targetId]) => targetId)
        .sort((a, b) => b[1] - a[1]);
    const winnerEntry = orderedResults[0];
    const highestPlayerVotes = winnerEntry ? winnerEntry[1] : 0;
    const hasTiedTopPlayers = highestPlayerVotes > 0
        && orderedResults.filter(([, votes]) => votes === highestPlayerVotes).length > 1;
    const skipWinsOrTies = voteBreakdown.skipVotes >= highestPlayerVotes;

    let outcome = {
        executed: false,
        targetId: null,
        targetName: null,
        voteSummary,
        voteBreakdown,
        isSkipped: true,
        eliminatedPlayer: null,
        candidates,
        eligibleVoterIds,
        message: 'VOTING SKIPPED - NO AGENT ELIMINATED'
    };

    // Trial votes are resolved by plurality, not absolute majority. A candidate
    // is executed only when they are the sole top-voted player and strictly beat
    // Skip. A player tie, a Skip tie/win, or no submitted candidate votes skips
    // the execution.
    if (winnerEntry && highestPlayerVotes > 0 && !hasTiedTopPlayers && !skipWinsOrTies) {
        const targetId = winnerEntry[0];
        const targetPlayer = targetRoom.players.find(player => player.id === targetId);
        // --- NEUROTOXIN-7: deliberately NOT checked here -------------------
        // The passive shield only negates a Killer's direct attack (see
        // 'kill_player'). Being voted out by the council is NOT a "fatal
        // blow" in that sense — Neurotoxin-7 grants no immunity from
        // elimination by vote, so a carried/unconsumed syringe is left
        // completely untouched by an execution and the player is eliminated
        // normally below.
        if (targetPlayer && !targetPlayer.isEliminated) {
            targetPlayer.isEliminated = true;
            targetPlayer.isObserver = true;

            // A council execution is public knowledge the instant it happens
            // (everyone sees 'trial_result') — so, unlike a Killer's victim,
            // no physical body is ever left behind in the mansion for this
            // player. Nothing gets pushed to targetRoom.bodies here: there is
            // nothing for a later 'search_body' (or simply walking into the
            // room) to find, in that room or any other, for the rest of the
            // match.

            outcome = {
                executed: true,
                targetId,
                targetName: targetPlayer.nickname,
                voteSummary,
                voteBreakdown,
                isSkipped: false,
                eliminatedPlayer: { id: targetId, nickname: targetPlayer.nickname },
                candidates,
                eligibleVoterIds,
                message: `${targetPlayer.nickname} has been executed by council vote.`
            };
            console.log(`Room ${targetRoom.code}: ${targetPlayer.nickname} eliminated by trial vote`);
        }
    }

    // The Joker wins the instant the council executes them (see the in-game
    // rules text: "Joker: Wins if compromised and executed by the council.").
    // Capture that here from the just-computed outcome so the resolution
    // timeout below can end the match instead of starting a new round.
    const executedRole = outcome.executed ? (targetRoom.roles?.[outcome.targetId] || null) : null;
    const jokerExecuted = executedRole === 'Joker';

    game.trial = {
        ...trial,
        status: 'resolved',
        candidates,
        voteSummary,
        skipVotes: voteBreakdown.skipVotes,
        result: outcome
    };

    game.phase = 'resolving';
    game.timelinePhase = 'TRIAL_RESOLUTION';
    game.phaseStartTime = Date.now();
    broadcastPhaseState(targetRoom);
    io.to(targetRoom.id).emit('trial_result', { code: targetRoom.code, ...outcome });
    io.to(targetRoom.id).emit('room_updated', {
        ...targetRoom,
        players: targetRoom.players,
        chatMessages: targetRoom.chatMessages || []
    });

    clearPhaseTimelineTimer(targetRoom.id);
    phaseTimelineTimers[targetRoom.id] = setTimeout(() => {
        if (targetRoom.game !== game || game.phase !== 'resolving') return;
        if (jokerExecuted) {
            endGameWithVictory(targetRoom, 'Joker', {
                reason: 'joker_executed',
                message: `${outcome.targetName} was the Joker and has been executed by council vote. The Joker wins!`,
                triggeredBy: outcome.targetId
            });
        } else if (executedRole === 'Killer') {
            // Win condition #2: the council has correctly identified and
            // executed the Killer. The match ends immediately in favor of
            // everyone else — Detective, Innocents, and any other peaceful/
            // special roles still standing — regardless of how many of them
            // remain, exactly like the existing "Innocents crack the code"
            // victory (see 'submit_innocent_code').
            endGameWithVictory(targetRoom, 'Innocent', {
                reason: 'killer_executed',
                message: `${outcome.targetName} was the Killer and has been executed by council vote. The Innocents win!`,
                triggeredBy: outcome.targetId
            });
        } else if (outcome.executed && checkKillerMajority(targetRoom)) {
            // Win condition #1: this execution just brought the number of
            // surviving peaceful players down to (or below) the number of
            // remaining Killer-team players. checkKillerMajority has already
            // ended the match in that case — nothing left to do here.
        } else {
            startNewRound(targetRoom);
        }
    }, TRIAL_RESOLUTION_MS);
}

// --- GAME OVER -------------------------------------------------------------
// Immediately ends the match in favor of `winner` — either 'Innocent' (cracking
// the override code, see 'submit_innocent_code') or 'Joker' (the Joker being
// executed by council vote during a trial, see resolveTrialPhase). Every
// active per-round/per-trial timer is torn down first so nothing left running
// from the round in progress can fire after the match has already ended.
function endGameWithVictory(targetRoom, winner, { reason, message, triggeredBy } = {}) {
    if (!targetRoom.game || targetRoom.game.phase === 'gameover') return;

    clearTurnTimer(targetRoom.id);
    clearTrialTickTimer(targetRoom.id);
    clearTrialTransitionTimers(targetRoom.id);
    clearGameOverTimer(targetRoom.id);

    targetRoom.game.phase = 'gameover';
    targetRoom.game.timelinePhase = 'GAME_OVER';
    targetRoom.game.phaseStartTime = Date.now();

    // Full role reveal is safe now — the match is over, so there's nothing left
    // to protect by keeping roles private.
    const roster = targetRoom.players.map(p => ({
        id: p.id,
        nickname: p.nickname,
        character: p.character,
        role: targetRoom.roles ? targetRoom.roles[p.id] || null : null,
        isEliminated: !!p.isEliminated
    }));

    io.to(targetRoom.id).emit('game_over', {
        code: targetRoom.code,
        winner,
        reason: reason || null,
        message: message || '',
        triggeredBy: triggeredBy || null,
        digitalCode: targetRoom.digitalCode || null,
        roster
    });

    console.log(`Room ${targetRoom.code}: GAME OVER — ${winner} team wins (${reason || 'n/a'})`);

    gameOverTimers[targetRoom.id] = setTimeout(() => {
        delete gameOverTimers[targetRoom.id];
        resetRoomToLobby(targetRoom);
    }, GAME_OVER_SUMMARY_MS);
}

// --- KILLER-TEAM NUMERIC MAJORITY WIN CONDITION -----------------------------
// Roles that fight alongside the Killer. An Accomplice only exists in 7+
// player matches (see SPECIAL_ROLES), but when present counts together with
// the Killer as one team for this check (e.g. a 2v2 standoff is still a
// Killer-team win, not a draw).
const KILLER_TEAM_ROLES = ['Killer', 'Accomplice'];

// The Joker is a solo, independent faction with its own separate win
// condition (being executed by the council — see the jokerExecuted branch in
// resolveTrialPhase). It is deliberately excluded from BOTH sides of the
// count below, so a lone surviving Joker can never itself tip this numeric
// check one way or the other. Every other role (Detective, Innocent, and the
// Officer/Forensic special roles, when in play) is "peaceful" for this count.
function isKillerTeamRole(role) {
    return KILLER_TEAM_ROLES.includes(role);
}

function isPeacefulRole(role) {
    return Boolean(role) && !KILLER_TEAM_ROLES.includes(role) && role !== 'Joker';
}

// --- ITEM: NEUROTOXIN-7 ------------------------------------------------
// One or more syringes planted once per match (see assignNeurotoxinLocation,
// called from assignRoles) in random searchable mansion rooms — how many
// scales with the room's player count (see neurotoxinCountForPlayerCount).
// There is no standalone pickup button: a syringe is discovered and
// resolved automatically as part of 'investigate_room' — whoever searches
// the room it's sitting in either picks it up or is told why they didn't
// (wrong role, or already carrying one — see the blocker below), all as a
// single popup instead of a plain toast (see the `neurotoxin` field
// attached to 'investigate_result').
//
// Only Killer / Accomplice / Joker may ever carry it; any other role is
// warned off and the syringe stays on the floor. A player may only ever
// hold ONE unconsumed syringe at a time — this is the "blocker": finding a
// second one while already carrying one leaves that second syringe exactly
// where it is (see findNeurotoxinEntry's use inside 'investigate_room').
//
//   - Killer:            grants a second kill within the SAME round (this
//                         game only ever gives a Killer one turn per round,
//                         so "within a round" and "within that turn" are
//                         the same window here — see 'kill_player'). The
//                         item is consumed only once the 2nd kill lands
//                         that round; a round with just 1 kill keeps it.
//   - Accomplice / Joker: a one-time passive shield. The next time this
//                         player would be eliminated (a Killer's attack OR
//                         a council execution), the elimination is negated
//                         instead and the item is consumed.
//
// All player-facing text is sent as an { en, ru } pair so the client can
// pick whichever matches its own active `language` state.
const NEUROTOXIN_ITEM_ID = 'item_neurotoxin';

const NEUROTOXIN_ITEM_DEFINITION = {
    id: NEUROTOXIN_ITEM_ID,
    name: { en: 'Neurotoxin-7', ru: 'Нейротоксин-7' },
    type: 'consumable'
};

// Only these three roles may ever carry Neurotoxin-7.
const NEUROTOXIN_ELIGIBLE_ROLES = ['Killer', 'Accomplice', 'Joker'];

// Kills needed within a single round before the item is consumed for a Killer.
const NEUROTOXIN_REQUIRED_KILLS = 2;

const NEUROTOXIN_MESSAGES = {
    hazardous: {
        en: 'The syringe contains an unknown dangerous compound — it is too hazardous to touch.',
        ru: 'Шприц содержит неизвестный ядовитый состав — без спецзащиты прикосновение смертельно опасно.'
    },
    pickedUpKiller: {
        en: 'You picked up Neurotoxin-7. Allows 2 kills in a single round.',
        ru: 'Вы подняли Нейротоксин-7. Позволяет совершить 2 убийства за один раунд.'
    },
    pickedUpShield: {
        en: 'You picked up Neurotoxin-7. Acts as a passive shield against fatal damage.',
        ru: 'Вы подняли Нейротоксин-7. Работает как пассивный щит от смертельного урона.'
    },
    firstKill: {
        en: 'First kill in this round executed. Second kill available!',
        ru: 'Первое убийство в этом раунде совершено. Доступно второе убийство!'
    },
    doubleKill: {
        en: 'Double kill executed! Neurotoxin consumed.',
        ru: 'Двойное убийство совершено! Нейротоксин израсходован.'
    },
    shieldTriggered: {
        en: 'Neurotoxin auto-injected! Fatal blow was negated.',
        ru: 'Автоматическая инъекция Нейротоксина! Смертельный удар нейтрализован.'
    },
    // Blocker: this player is already carrying an unconsumed syringe, so a
    // second one found in another room cannot be picked up — it's left
    // exactly where it is for someone else.
    alreadyCarrying: {
        en: "There's another Neurotoxin-7 syringe here, but you're already carrying one — you can only hold a single dose at a time.",
        ru: 'Здесь ещё один шприц с Нейротоксином-7, но у вас уже есть один — с собой можно нести только одну дозу.'
    }
};

function ensureInventory(player) {
    if (!Array.isArray(player.inventory)) player.inventory = [];
    return player.inventory;
}

function findNeurotoxinEntry(player) {
    if (!player) return null;
    return ensureInventory(player).find(entry => entry.itemId === NEUROTOXIN_ITEM_ID) || null;
}

function removeNeurotoxinEntry(player) {
    const inventory = ensureInventory(player);
    const idx = inventory.findIndex(entry => entry.itemId === NEUROTOXIN_ITEM_ID);
    if (idx !== -1) inventory.splice(idx, 1);
}

// How many kills a Killer may make THIS turn — 2 while carrying an
// unconsumed Neurotoxin-7, otherwise the normal 1.
function killerMaxKillsThisTurn(killerPlayer) {
    return findNeurotoxinEntry(killerPlayer) ? NEUROTOXIN_REQUIRED_KILLS : 1;
}

// How many Neurotoxin-7 syringes are in play for the match, scaled to the
// room's player count — bigger lobbies spread more copies around so a
// single scramble for one item doesn't dominate a 10-12 player match:
//   5-7 players  -> 1 syringe
//   8-10 players -> 2 syringes
//   11-12 players -> 3 syringes
function neurotoxinCountForPlayerCount(playerCount) {
    if (playerCount <= 7) return 1;
    if (playerCount <= 10) return 2;
    return 3;
}

// Plants this match's Neurotoxin-7 syringe(s) in distinct random searchable
// mansion rooms — how many is decided by neurotoxinCountForPlayerCount.
// Called once, right after roles are assigned (see assignRoles) — guarded
// there so a stray re-entry can never re-roll them mid-match, same pattern
// as assignEvidenceLocations/digitalCode.
function assignNeurotoxinLocation(targetRoom) {
    const candidates = shuffleArray(allSearchableRoomIds());
    const count = Math.min(neurotoxinCountForPlayerCount(targetRoom.players.length), candidates.length);
    targetRoom.neurotoxinLocations = candidates.slice(0, count).map(roomId => ({
        itemId: NEUROTOXIN_ITEM_ID,
        roomId,
        pickedUp: false
    }));
    const roomNames = targetRoom.neurotoxinLocations.map(loc => findMansionRoomById(loc.roomId)?.name || loc.roomId).join(', ');
    console.log(`Neurotoxin-7: planted ${count} syringe(s) for room ${targetRoom.code} (${targetRoom.players.length} players) in: ${roomNames}`);
}

// Finds an unclaimed syringe sitting in a specific mansion room, if any —
// used by 'investigate_room' to fold pickup into that action instead of a
// separate button/event.
function findNeurotoxinLocationInRoom(targetRoom, roomId) {
    if (!Array.isArray(targetRoom.neurotoxinLocations)) return null;
    return targetRoom.neurotoxinLocations.find(loc => loc.roomId === roomId && !loc.pickedUp) || null;
}

// Resolves Neurotoxin-7's passive shield for Accomplice/Joker. Call this
// BEFORE marking a target eliminated by a Killer's direct attack (see
// 'kill_player'). Deliberately NOT used for a council/vote execution —
// Neurotoxin-7 grants no immunity from being voted out (see
// resolveTrialPhase, which eliminates on a winning vote unconditionally).
//
// Returns true  -> the hit was negated, item consumed, do NOT eliminate.
// Returns false -> no shield available, proceed with the elimination.
function tryNeurotoxinShield(targetRoom, victimPlayer) {
    const role = targetRoom.roles ? targetRoom.roles[victimPlayer.id] : undefined;
    if (role !== 'Accomplice' && role !== 'Joker') return false;

    const entry = findNeurotoxinEntry(victimPlayer);
    if (!entry) return false;

    removeNeurotoxinEntry(victimPlayer);

    console.log(`Neurotoxin-7: room=${targetRoom.code} ${role} ${victimPlayer.id} shield triggered — item consumed`);

    io.to(victimPlayer.id).emit('player:takeFatalHit:result', {
        code: targetRoom.code,
        negated: true,
        message: NEUROTOXIN_MESSAGES.shieldTriggered
    });

    return true;
}

// Win condition #1: the moment the number of still-active peaceful players
// (Detective + Innocents, plus Officer/Forensic if in play) drops to or below
// the number of still-active Killer-team players (Killer, plus Accomplice if
// in play), the Killer team has effectively already won — they can no longer
// be outvoted/outnumbered. Called after every elimination that could create
// this parity: a Killer's kill (see 'kill_player') and a council execution
// (see resolveTrialPhase). Ends the match immediately when true and returns
// true so the caller knows to stop whatever it would otherwise do next
// (starting a new round, etc.); returns false if the match continues.
function checkKillerMajority(targetRoom) {
    const game = targetRoom.game;
    if (!game || game.phase === 'gameover') return false;

    const roles = targetRoom.roles || {};
    const activePlayers = targetRoom.players.filter(p => !p.isEliminated && !p.isObserver);

    const aliveKillerTeam = activePlayers.filter(p => isKillerTeamRole(roles[p.id])).length;
    const alivePeaceful = activePlayers.filter(p => isPeacefulRole(roles[p.id])).length;

    // aliveKillerTeam > 0 guards the (should-never-happen) case of the Killer
    // already being gone, which is handled separately by the "Killer executed"
    // win condition instead.
    if (aliveKillerTeam > 0 && alivePeaceful <= aliveKillerTeam) {
        endGameWithVictory(targetRoom, 'Killer', {
            reason: 'killer_majority',
            message: alivePeaceful <= 0
                ? 'Every peaceful agent has been eliminated. The Killer wins!'
                : 'The Killer team now equals or outnumbers the remaining peaceful agents. The Killer wins!'
        });
        return true;
    }

    // Win condition #3: the entire Killer team (Killer, plus Accomplice when
    // in play) is no longer active in the match. In practice this almost
    // always means they disconnected/left rather than being executed — a
    // council execution already ends the match via the 'killer_executed'
    // branch in resolveTrialPhase, so by the time we get here that path has
    // already fired. With nobody left to hunt the peaceful team, there is
    // nothing left to play for, so the win is handed to whichever peaceful
    // agents remain, same as a correct execution of the Killer would give.
    if (aliveKillerTeam === 0 && alivePeaceful > 0) {
        endGameWithVictory(targetRoom, 'Innocent', {
            reason: 'killer_team_disconnected',
            message: 'The Killer is no longer in the match. The Innocents win!'
        });
        return true;
    }

    return false;
}

// Fully resets a room back to its pre-match lobby state after a GAME_OVER
// summary has had time to display, so the same room/players can immediately
// start a new match. Reuses the 'room_joined' event/payload shape — every
// client already knows how to handle that (see onRoomJoined), so no new
// frontend event is needed just to move everyone back to the lobby screen.
function resetRoomToLobby(targetRoom) {
    clearTurnTimer(targetRoom.id);
    clearTrialTickTimer(targetRoom.id);
    clearTrialTransitionTimers(targetRoom.id);
    clearGameOverTimer(targetRoom.id);

    targetRoom.status = 'open';
    targetRoom.game = null;
    targetRoom.roles = {};
    targetRoom.digitalCode = null;
    targetRoom.evidenceLocations = null;
    // BUGFIX: this was missing, so the one-shot-per-match guard in
    // assignRoles (`if (!targetRoom.neurotoxinLocations)`) saw last match's
    // array still sitting here and skipped planting new syringes entirely —
    // Neurotoxin-7 would only ever appear in the very first match a room
    // played, never in any rematch. Clearing it here is what makes a fresh
    // assignNeurotoxinLocation() run for every new match, same as
    // evidenceLocations above.
    targetRoom.neurotoxinLocations = null;
    targetRoom.innocentClearedRooms = null;
    targetRoom.plantedEvidence = null;
    targetRoom.traps = null;
    targetRoom.bodies = null;
    targetRoom.discoveredBodies = null;
    targetRoom.discoveredClues = null;
    targetRoom.introStarted = false;
    targetRoom.loadedPlayers = [];
    targetRoom.roleRevealConfirmed = [];
    targetRoom.skipVotes = [];

    targetRoom.players.forEach(p => {
        p.isReady = false;
        p.isEliminated = false;
        p.isObserver = false;
        p.character = null;
    });

    console.log(`Room ${targetRoom.code}: reset to lobby after GAME_OVER`);

    io.to(targetRoom.id).emit('room_joined', {
        roomId: targetRoom.id,
        roomCode: targetRoom.code,
        roomName: targetRoom.name,
        type: targetRoom.type,
        hostId: targetRoom.hostId,
        status: targetRoom.status,
        players: targetRoom.players,
        minPlayers: MIN_PLAYERS,
        maxPlayers: MAX_PLAYERS,
        chatMessages: targetRoom.chatMessages || []
    });

    if (targetRoom.type === 'public') {
        io.emit('rooms_list', publicRoomsList());
    }
}

// Starts a new round: shuffles the turn order (guaranteed different from the
// previous one) and starts the first player's turn in that order.
function startNewRound(targetRoom) {
    const game = targetRoom.game;
    if (!game) return;

    // A body that's already been found (foundBy non-empty) has, by this point,
    // been through at least one Court/Trial phase — its discovery is already
    // reflected in the match's findings recap (see buildFindingsSummary) and
    // in every player's memory of that trial. It has no further reason to go
    // on lying around in its room forever: leaving it there meant anyone who
    // walked back in (or searched again) kept finding the same already-known
    // body every single round for the rest of the match. Only STILL-UNDISCOVERED
    // bodies (foundBy empty) carry over into the new round — those genuinely
    // haven't been dealt with yet.
    if (targetRoom.bodies) {
        if (!targetRoom.discoveredBodies) targetRoom.discoveredBodies = [];
        const justDiscovered = targetRoom.bodies.filter(body => body.foundBy && body.foundBy.length > 0);
        targetRoom.discoveredBodies.push(...justDiscovered);
        targetRoom.bodies = targetRoom.bodies.filter(body => !body.foundBy || body.foundBy.length === 0);
    }

    // Same reasoning applies to Joker-planted evidence: once a clue has been
    // found (foundBy non-empty), it's already been folded into the shared
    // CLUES board and the trial findings recap (see buildCluesBoard /
    // buildFindingsSummary) — permanent, match-lifetime records that don't
    // depend on the item still sitting in its room. Leaving a found clue in
    // place meant anyone re-searching that room kept "discovering" the same
    // already-known evidence every round for the rest of the match. Only
    // still-undiscovered clues (foundBy empty) carry over into the new round.
    if (targetRoom.plantedEvidence) {
        if (!targetRoom.discoveredClues) targetRoom.discoveredClues = [];
        Object.keys(targetRoom.plantedEvidence).forEach(roomId => {
            const justDiscovered = targetRoom.plantedEvidence[roomId].filter(
                entry => entry.foundBy && entry.foundBy.length > 0
            );
            targetRoom.discoveredClues.push(...justDiscovered);
            targetRoom.plantedEvidence[roomId] = targetRoom.plantedEvidence[roomId].filter(
                entry => !entry.foundBy || entry.foundBy.length === 0
            );
        });
    }

    // TODO: once elimination mechanics exist, filter here to only living players
    // instead of targetRoom.players as a whole.
    const alivePlayerIds = targetRoom.players.filter(p => !p.isEliminated && !p.isObserver).map(p => p.id);

    game.round += 1;
    game.phase = 'action';
    game.timelinePhase = 'EXPLORATION';
    game.phaseStartTime = Date.now();
    game.turnOrder = shuffleTurnOrder(alivePlayerIds, game.turnOrder);
    game.currentTurnIndex = 0;

    // Fog of war in the mansion resets every new round: who was seen in which room
    // last round doesn't carry over — otherwise "who you found in the room" would
    // quickly stop meaning anything (rooms would keep accumulating players forever).
    game.playerLocations = {};
    game.roomOccupants = {};
    game.spectatorViews = {};
    game.ventUsedThisTurn = {};
    // Whoever's turn is up gets a brand-new, unlocked room-interaction phase —
    // see 'search_body' / 'investigate_room' for how this gets consumed and
    // 'select_room' / 'use_vent' for how entering a room re-arms it mid-turn.
    game.roomActionUsedThisTurn = {};
    // See the 'investigateRoomUsedThisTurn' field comment in startGame — reset
    // alongside roomActionUsedThisTurn for the same reason.
    game.investigateRoomUsedThisTurn = {};
    // A Killer gets exactly one kill per turn — see 'kill_player' for the guard.
    // Unlike roomActionUsedThisTurn, this is NOT re-armed by entering a fresh
    // room (via 'select_room' or a vent hop): a Killer who kills, resolves the
    // body, and then walks/vents into a room with another potential victim
    // must not be able to kill again until their NEXT turn.
    game.killUsedThisTurn = {};

    // --- NEUROTOXIN-7: reset the per-round kill counter --------------------
    // Only reaching 2 kills WITHIN a round consumes the item (see
    // 'kill_player') — a Killer who landed just 1 kill last round keeps it
    // going into this new round, so the item itself is deliberately left
    // untouched here.
    targetRoom.players.forEach(player => {
        const entry = findNeurotoxinEntry(player);
        if (entry) entry.killsInCurrentRound = 0;
    });

    // A kill's post-kill body decision (see 'kill_player' / 'resolve_kill') must
    // always be settled before its own turn ends (advanceTurn auto-resolves it
    // to "expose" if not) — never something a Killer carries into a new round.
    game.pendingKillDecision = null;
    // Do not retain selections from a completed vote into the next round.
    game.trial = null;

    // --- OFFICER: apply a scheduled "Lock in Holding Cell" for this round ---
    // A lock scheduled by 'officer_lock_player' during a PREVIOUS round's
    // trial (see holdingCellLock) takes effect starting exactly the round it
    // was scheduled for: the target is pre-placed in f1_holding_cell before
    // their turn ever starts, so the ordinary 'select_room' guard ("already
    // searched a room this turn") naturally confines them there for the
    // round's entire action phase — no separate movement restriction needed.
    game.lockedInHoldingCellPlayerId = null;
    if (game.holdingCellLock && game.holdingCellLock.lockedForRound === game.round) {
        const lockedTarget = targetRoom.players.find(p => p.id === game.holdingCellLock.targetId);
        if (lockedTarget && !lockedTarget.isEliminated && !lockedTarget.isObserver) {
            game.playerLocations[lockedTarget.id] = 'f1_holding_cell';
            game.roomOccupants['f1_holding_cell'] = [lockedTarget.id];
            game.lockedInHoldingCellPlayerId = lockedTarget.id;
            console.log(`Room ${targetRoom.code}: ${lockedTarget.nickname} is LOCKED IN THE HOLDING CELL for round ${game.round}`);
        }
    }
    // One-shot schedule: whether just applied above or stale (e.g. the target
    // left/was eliminated before it could take effect), it never carries
    // forward into a later round.
    if (game.holdingCellLock && game.holdingCellLock.lockedForRound <= game.round) {
        game.holdingCellLock = null;
    }

    io.to(targetRoom.id).emit('round_start', {
        code: targetRoom.code,
        round: game.round,
        turnOrder: game.turnOrder,
        // Public: everyone can see this seat empty for the whole round, unlike
        // the Officer's use of the ability itself (kept private — see
        // 'officer_lock_result').
        lockedInHoldingCell: game.lockedInHoldingCellPlayerId
            ? {
                id: game.lockedInHoldingCellPlayerId,
                nickname: (targetRoom.players.find(p => p.id === game.lockedInHoldingCellPlayerId) || {}).nickname || 'Agent'
            }
            : null
    });
    broadcastPhaseState(targetRoom);

    // Privately tell any trap-debuffed player (see triggerTrapIfPresent /
    // isPlayerTrapDebuffed) that this is their penalty round — sent to every
    // active player so the "no longer debuffed" case also refreshes their
    // client state, same as 'trap_status' already does for the Accomplice.
    targetRoom.players.forEach(p => emitTrapDebuffStatus(targetRoom, p.id));

    console.log(`Room ${targetRoom.code}: ROUND ${game.round} started, order=`, game.turnOrder);
    startPlayerTurn(targetRoom);
}

// Initializes the room's game state and starts the first round.
// Called once, when ALL players have confirmed they've finished watching their role reveal.
function startGame(targetRoom) {
    targetRoom.game = {
        round: 0,
        phase: 'action',
        timelinePhase: 'EXPLORATION',
        phaseStartTime: Date.now(),
        turnOrder: [],
        currentTurnIndex: 0,
        turnEndsAt: null,
        turnToken: 0,
        // Forensic Examiner match-persistent clue/report state: each discovered
        // body keeps its own fixed forensic clue, and each Forensic player keeps
        // a private history of the bodies they have examined.
        forensicMasterResult: null,
        forensicAbilityLastUsedRound: {},
        forensicReports: {},
        // playerLocations: playerId -> roomId the player "entered" this round.
        // roomOccupants: roomId -> [playerId, ...] who has already checked in there
        // this round — used to show a later-arriving player who else was found in
        // the same room (see 'select_room').
        playerLocations: {},
        roomOccupants: {},
        spectatorViews: {},
        // ventUsedThisTurn: playerId -> true once the Killer has used a vent
        // this turn. Reset every round alongside playerLocations, and covers
        // BOTH climbing back through the same vent and hopping into a
        // different one — a single vent trip is the whole action.
        ventUsedThisTurn: {},
        // roomActionUsedThisTurn: playerId -> true once they've spent this turn's
        // single room-interaction phase on EITHER 'search_body' or 'investigate_room'
        // (see both handlers below). The two are mutually exclusive: whichever one
        // fires first consumes the phase and locks out the other until the player
        // enters a fresh room (a new 'select_room', or a mid-turn 'use_vent' hop —
        // both clear this flag) or their next turn starts.
        roomActionUsedThisTurn: {},
        // investigateRoomUsedThisTurn: playerId -> true once they've used
        // 'investigate_room' on the room they're CURRENTLY standing in. Gates
        // 'check_room' (Mark Room / "CHECK ROOM" button) below — an Innocent
        // must actually investigate a room before they're allowed to mark it
        // checked. Reset alongside roomActionUsedThisTurn: at the start of
        // every round, and re-armed to false whenever a fresh room is entered
        // (a new 'select_room' or a mid-turn 'use_vent' hop), since the
        // requirement is per-room, not just once per turn.
        investigateRoomUsedThisTurn: {},
        // killUsedThisTurn: playerId -> true once the Killer has landed a kill
        // this turn. A Killer gets exactly one kill per turn, no matter how many
        // rooms they pass through afterward (see 'kill_player'). Reset every
        // round alongside the other once-per-turn flags above.
        killUsedThisTurn: {},
        // pendingKillDecision: null, or { killerId, targetId, targetNickname,
        // targetCharacter, roomId } the instant the Killer's target has been eliminated
        // (see 'kill_player') but before they've chosen what happens to the body
        // (see 'resolve_kill'). While set, every other turn action for that
        // killer is locked out — see the guards in 'select_room', 'use_vent',
        // 'investigate_room', 'search_body' and 'end_turn' below.
        pendingKillDecision: null,
        // jokerOwnTurnCount: playerId -> how many of THEIR OWN turns have started
        // so far this match. jokerClueLastUsedOwnTurn: playerId -> the value of
        // that counter the last time they planted evidence (see
        // 'plant_joker_evidence' / JOKER_CLUE_COOLDOWN_TURNS). jokerCluePoolByPlayer:
        // playerId -> a shuffled queue of not-yet-used CHARACTER_EVIDENCE indices,
        // consumed one at a time and reshuffled from scratch once emptied, so
        // clues come out in random order without repeating within a cycle. All
        // three are only ever populated for whoever holds the Joker role.
        jokerOwnTurnCount: {},
        jokerClueLastUsedOwnTurn: {},
        jokerCluePoolByPlayer: {},
        // Same shape as the Joker trio above, but for the Accomplice's "Change
        // Evidence" ability (see 'accomplice_change_evidence' /
        // ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS). Only ever populated for whoever
        // holds the Accomplice role.
        accompliceOwnTurnCount: {},
        accompliceEvidenceLastUsedOwnTurn: {},
        // --- DETECTIVE: "CHECK PLAYER'S LAST LOCATION" (Court/Trial phase only) --
        // lastKnownEndRoom: playerId -> roomId, a snapshot of game.playerLocations
        // taken the instant the round's movement phase ends and the Court/Trial
        // phase begins (see startTrialPhase). Kept as its own field (rather than
        // just reading game.playerLocations directly) so the Detective's answer
        // stays stable and well-defined for the whole trial even though
        // playerLocations conceptually belongs to the (now-finished) action phase.
        lastKnownEndRoom: {},
        // detectiveAbilityLastUsedRound: playerId -> the game.round the Detective
        // last used the ability on. Persists across rounds (NOT reset in
        // startNewRound, unlike the once-per-round flags above) since the
        // cooldown is measured in whole rounds via DETECTIVE_ABILITY_COOLDOWN_ROUNDS.
        detectiveAbilityLastUsedRound: {},
        // --- OFFICER: "LOCK IN HOLDING CELL" (Court/Trial phase only) ----------
        // officerAbilityLastUsedRound: playerId -> the game.round the Officer
        // last used the ability on. Persists across rounds (NOT reset in
        // startNewRound), same treatment as detectiveAbilityLastUsedRound above,
        // since the cooldown is measured in whole rounds via
        // OFFICER_ABILITY_COOLDOWN_ROUNDS.
        officerAbilityLastUsedRound: {},
        // --- INNOCENT: "CHECK ROOM" (Mark Room, action phase) ------------------
        // markRoomLastUsedRound: playerId -> the game.round the Innocent last
        // used 'check_room' on. Persists across rounds (NOT reset in
        // startNewRound), same treatment as officerAbilityLastUsedRound above,
        // since the cooldown is measured in whole rounds via
        // MARK_ROOM_COOLDOWN_ROUNDS.
        markRoomLastUsedRound: {},
        // holdingCellLock: null, or { targetId, targetNickname, lockedForRound }
        // set the instant the Officer uses the ability (see
        // 'officer_lock_player'). lockedForRound is always game.round + 1 at
        // that moment — the lock never applies to the round it was used in,
        // only the NEXT one. Consumed (and cleared) by startNewRound once
        // game.round reaches lockedForRound; persists across the one round
        // in between so it survives the trial -> new-round transition.
        holdingCellLock: null,
        // lockedInHoldingCellPlayerId: null, or the playerId actually confined
        // to f1_holding_cell FOR THE CURRENT ROUND. Derived from
        // holdingCellLock at the start of every round (see startNewRound) and
        // reset to null every round — unlike the ability's use itself (private
        // to the Officer), this is public: every player can see that seat sit
        // empty all round, so it rides along on the public 'round_start' event.
        lockedInHoldingCellPlayerId: null,
        trial: null
    };

    // roomId -> [{ id, text, roomId, roomName, round, plantedAt }, ...] planted by
    // the Joker over the course of the match. Lives on the room (like
    // evidenceLocations/digitalCode), not the per-round game state, so a clue
    // stays put across rounds until the match ends.
    targetRoom.plantedEvidence = {};

    // roomId -> [{ id, roomId, roomName, round, setBy, setAt }, ...] pinned by
    // the Accomplice via 'set_trap' (mirrors plantedEvidence above — lives on
    // the room, not the per-round game state, so a trap stays put across
    // rounds until the match ends). Purely inert for now: nothing currently
    // reads this to trigger any effect when another player walks into a
    // trapped room — it's just recorded and shown back to the Accomplice.
    targetRoom.traps = {};

    // [{ playerId, nickname, roomId, round, foundBy: [] }, ...] one entry per
    // player killed by the Killer (see 'resolve_kill') and left behind in
    // whichever mansion room the kill happened in. Council-executed players
    // never get an entry here (see resolveTrialPhase) — their execution is
    // already public, and per design their "body" isn't a thing anyone can
    // go find. Lives on the room rather than the per-round game state — same
    // reasoning as plantedEvidence above — so a body stays put in that room
    // for the rest of the match, available to 'search_body' in any later round.
    targetRoom.bodies = [];

    // Permanent, append-only archives of every body/clue that has EVER been
    // discovered, for the entire match. targetRoom.bodies / plantedEvidence
    // get pruned of already-found entries at the start of every new round
    // (see startNewRound) so a stale body/clue doesn't keep re-triggering
    // "found it!" toasts on revisit — but that means those two live arrays
    // are NOT a valid source of truth for "everything discovered so far".
    // buildFindingsSummary / buildCluesBoard read from these archives
    // (merged with whatever's still pending removal in the current round)
    // instead, so a discovered body/clue never disappears from the
    // BODIES/CLUES panels once the round rolls over.
    targetRoom.discoveredBodies = [];
    targetRoom.discoveredClues = [];

    // roomId -> entityId -> {x, y}. See getEntityPosition. Reset here so a
    // brand-new match doesn't inherit spots from a previous game in this room.
    targetRoom.entityPositions = {};

    io.to(targetRoom.id).emit('game_started', { code: targetRoom.code });
    broadcastExitStatus(targetRoom);
    console.log(`Room ${targetRoom.code}: GAME STARTED`);
    startNewRound(targetRoom);
}

// Assigns roles according to the lobby size. At 5–7 players the game uses only
// the base trio (Killer, Detective, Officer) plus Innocents, adding one more
// Innocent per extra player; at 8–12 the complete unique-role pool is used.
// Sends each player ONLY their own role, privately (via their socket.id room), so
// other players' roles never leak into the traffic.
function assignRoles(targetRoom) {
    const shuffledPlayers = shuffleArray(targetRoom.players);
    targetRoom.roles = {};
    const rolePool = targetRoom.players.length > 7
        ? [...BASE_ROLES, ...SPECIAL_ROLES]
        : BASE_ROLES;

    shuffledPlayers.forEach((player, index) => {
        const role = index < rolePool.length ? rolePool[index] : 'Innocent';
        targetRoom.roles[player.id] = role;
    });

    console.log(`Roles assigned for room ${targetRoom.code}:`, targetRoom.roles);

    // Evidence locations are generated ONCE per match, right here — the very
    // first (and only) time roles are known. Guarded so a stray re-entry into
    // assignRoles can never reshuffle the fragments mid-match.
    if (!targetRoom.evidenceLocations) {
        assignEvidenceLocations(targetRoom);
    }

    // Same one-shot-per-match guard as evidenceLocations above.
    if (!targetRoom.neurotoxinLocations) {
        assignNeurotoxinLocation(targetRoom);
    }

    targetRoom.players.forEach(player => {
        io.to(player.id).emit('role_assigned', { role: targetRoom.roles[player.id] });
    });
}

// Shared logic for when a player leaves a room (leave_room OR disconnect):
// - if the leaver was the host, a new host is picked RANDOMLY among the remaining players
// - if the room was in "preparing" mode, roll it back to 'open' and reset everyone's isReady
// - in any case, cancel the active countdown if one was running
function handlePlayerLeftRoom(targetRoom, leavingSocketId) {
    cancelCountdown(targetRoom);

    if (targetRoom.players.length === 0) {
        clearTurnTimer(targetRoom.id);
        clearTrialTickTimer(targetRoom.id);
        return;
    }

    if (targetRoom.hostId === leavingSocketId) {
        // A random player among those remaining becomes the new host
        const randomIndex = Math.floor(Math.random() * targetRoom.players.length);
        targetRoom.hostId = targetRoom.players[randomIndex].id;
        console.log(`Room ${targetRoom.code}: host left, new host is ${targetRoom.hostId} (randomly selected)`);
    }

    if (targetRoom.status === 'preparing' && !targetRoom.introStarted) {
        targetRoom.status = 'open';
        targetRoom.players.forEach(p => { p.isReady = false; });
        console.log(`Room ${targetRoom.code} reverted to OPEN — a player left during preparation`);
    }

    // If someone left mid-game (after game_start), clean up the load/skip lists so
    // "everyone loaded" / "everyone voted" conditions don't hang forever because of
    // a player who's no longer there.
    if (targetRoom.loadedPlayers) {
        targetRoom.loadedPlayers = targetRoom.loadedPlayers.filter(id => id !== leavingSocketId);
    }
    if (targetRoom.skipVotes) {
        targetRoom.skipVotes = targetRoom.skipVotes.filter(id => id !== leavingSocketId);
    }
    if (targetRoom.roleRevealConfirmed) {
        targetRoom.roleRevealConfirmed = targetRoom.roleRevealConfirmed.filter(id => id !== leavingSocketId);
    }

    // If the game is already running and the leaver was in the turn order, remove
    // them from it; if they were specifically the current player, immediately
    // advance the turn (or move to trial if the order is now empty), so the game
    // doesn't hang waiting on a turn from someone who's gone.
    if (targetRoom.game && targetRoom.game.turnOrder.includes(leavingSocketId)) {
        const game = targetRoom.game;
        const leavingIndex = game.turnOrder.indexOf(leavingSocketId);
        const wasCurrentPlayer = leavingIndex === game.currentTurnIndex && game.phase === 'action';

        game.turnOrder = game.turnOrder.filter(id => id !== leavingSocketId);
        if (leavingIndex < game.currentTurnIndex) {
            game.currentTurnIndex -= 1;
        }

        if (wasCurrentPlayer && game.phase === 'action') {
            if (game.turnOrder.length === 0) {
                startTrialPhase(targetRoom);
            } else {
                if (game.currentTurnIndex >= game.turnOrder.length) {
                    game.currentTurnIndex = 0;
                }
                startPlayerTurn(targetRoom);
            }
        }
    }

    // A departure can make the remaining confirmation set complete.  Ballots stay
    // private; only the IDs of players who locked a ballot are broadcast.
    if (targetRoom.game && targetRoom.game.phase === 'trial' && targetRoom.game.trial) {
        const trial = targetRoom.game.trial;
        delete trial.votes[leavingSocketId];
        trial.confirmedVoterIds = (trial.confirmedVoterIds || []).filter(id => id !== leavingSocketId);
        trial.eligibleVoterIds = (trial.eligibleVoterIds || []).filter(id => id !== leavingSocketId);
        const voteBreakdown = buildTrialVoteBreakdown(trial, trial.eligibleVoterIds);
        trial.voteSummary = voteBreakdown.playerVotes;
        trial.skipVotes = voteBreakdown.skipVotes;
        trial.candidates = buildTrialCandidates(targetRoom, trial.eligibleVoterIds, trial.voteSummary);

        io.to(targetRoom.id).emit('trial_vote_update', {
            code: targetRoom.code,
            confirmedVoterIds: trial.confirmedVoterIds,
            totalEligible: trial.eligibleVoterIds.length
        });
        io.to(targetRoom.id).emit('trial_roster_update', {
            code: targetRoom.code,
            players: buildTrialRoster(targetRoom)
        });
        broadcastTrialPlayerList(targetRoom);

        if (trial.eligibleVoterIds.length === 0 || trial.eligibleVoterIds.every(id => trial.confirmedVoterIds.includes(id))) {
            resolveTrialPhase(targetRoom);
        }
    }

    // If the leaver had already been recorded as physically "in" a mansion room
    // this round, scrub them from that room's occupant list too — otherwise a
    // ghost entry could keep showing up to whoever searches that room next.
    if (targetRoom.game && targetRoom.game.roomOccupants) {
        Object.keys(targetRoom.game.roomOccupants).forEach(roomId => {
            targetRoom.game.roomOccupants[roomId] = targetRoom.game.roomOccupants[roomId].filter(id => id !== leavingSocketId);
        });
    }
    if (targetRoom.game && targetRoom.game.playerLocations) {
        delete targetRoom.game.playerLocations[leavingSocketId];
    }
    if (targetRoom.game && targetRoom.game.ventUsedThisTurn) {
        delete targetRoom.game.ventUsedThisTurn[leavingSocketId];
    }
    if (targetRoom.game && targetRoom.game.roomActionUsedThisTurn) {
        delete targetRoom.game.roomActionUsedThisTurn[leavingSocketId];
    }
    if (targetRoom.game && targetRoom.game.investigateRoomUsedThisTurn) {
        delete targetRoom.game.investigateRoomUsedThisTurn[leavingSocketId];
    }
    if (targetRoom.game && targetRoom.game.killUsedThisTurn) {
        delete targetRoom.game.killUsedThisTurn[leavingSocketId];
    }
    if (targetRoom.game && targetRoom.game.spectatorViews) {
        delete targetRoom.game.spectatorViews[leavingSocketId];
        [...new Set(Object.values(targetRoom.game.spectatorViews))].forEach(roomId => {
            emitSpectatorRoomUpdates(targetRoom, roomId);
        });
    }
    // A Killer who disconnects mid-decision can never come back to resolve it
    // this turn — default to leaving the body exposed rather than losing track
    // of it, same fallback 'advanceTurn' uses for a timed-out decision.
    if (targetRoom.game && targetRoom.game.pendingKillDecision && targetRoom.game.pendingKillDecision.killerId === leavingSocketId) {
        const pending = targetRoom.game.pendingKillDecision;
        if (!targetRoom.bodies) targetRoom.bodies = [];
        targetRoom.bodies.push({
            playerId: pending.targetId,
            nickname: pending.targetNickname,
            character: pending.targetCharacter || null,
            roomId: pending.roomId,
            round: targetRoom.game.round,
            isHidden: false,
            foundBy: []
        });
        console.log(`Room ${targetRoom.code}: post-kill decision auto-resolved as "expose" (killer disconnected) for ${pending.targetNickname}`);
        emitSpectatorRoomUpdates(targetRoom, pending.roomId);
        broadcastExitStatus(targetRoom);
        targetRoom.game.pendingKillDecision = null;
    }

    // A player leaving/disconnecting mid-match changes the surviving head
    // count exactly like a kill or a council execution does — without this,
    // a match could sit at (say) 1 Innocent vs 0 Killer, or 1 peaceful vs 1
    // Killer, forever, never actually resolving, since nothing else ever
    // re-runs this check outside of 'kill_player' and a trial's outcome.
    // checkKillerMajority is a no-op (returns false) if there's no game yet
    // or the match is already over, so this is always safe to call here.
    if (targetRoom.game) {
        checkKillerMajority(targetRoom);
    }
}

io.on('connection', (socket) => {
    console.log(`Player connected: ${socket.id}`);

    socket.on('get_public_rooms', () => {
        socket.emit('rooms_list', publicRoomsList());
    });

    // Recovery path for clients that mounted after the trial-start broadcast.
    // `code` is optional so a client already joined to the room can simply ask.
    socket.on('request_trial_players', ({ code } = {}) => {
        const targetRoom = Object.values(rooms).find(room =>
            room.game?.phase === 'trial' &&
            (!code || room.code === code) &&
            room.players.some(player => player.id === socket.id)
        );

        if (targetRoom) {
            socket.emit('trial_player_list', buildTrialRoster(targetRoom));
        }
    });

    // Rejoining or late-mounting clients can request the exact server-owned
    // micro-phase instead of guessing from stale local timers.
    socket.on('request_phase_state', ({ code } = {}) => {
        const targetRoom = Object.values(rooms).find(room =>
            room.game && (!code || room.code === code) &&
            room.players.some(player => player.id === socket.id)
        );
        if (targetRoom) {
            sendPhaseState(socket, targetRoom);
            // Re-sync the trap debuff (see isPlayerTrapDebuffed) on
            // reconnect/refresh, same as any other private per-player status.
            emitTrapDebuffStatus(targetRoom, socket.id);
        }
    });

    socket.on('create_room', ({ type, nickname }) => {
        const roomCode = generateUniqueRoomCode();
        const roomName = generateUniqueRoomName();
        // roomCode is already guaranteed unique (checked in a loop inside
        // generateUniqueRoomCode), so roomId is built from it rather than
        // Date.now(). Previously, creating two rooms within the same millisecond
        // caused the second create_room to overwrite rooms[roomId] of the first —
        // leaving the wrong socket as host.
        const roomId = `room_${roomCode}`;

        rooms[roomId] = {
            id: roomId,
            code: roomCode,
            name: roomName,
            type: type,
            hostId: socket.id,
            status: 'open', // 'open' -> can join | 'preparing' -> join locked, preparation/game in progress
            players: [{ id: socket.id, nickname: nickname || 'Agent', character: null, isReady: false, isEliminated: false, isObserver: false, inventory: [] }],
            loadedPlayers: [],
            skipVotes: [],
            introStarted: false,
            roles: {},
            roleRevealConfirmed: [],
            game: null,
            chatMessages: []
        };

        socket.join(roomId);

        console.log(`Room created: ${roomId} (${roomCode}) by ${socket.id} (host)`);

        socket.emit('room_joined', {
            roomId,
            roomCode,
            roomName,
            type,
            hostId: rooms[roomId].hostId,
            status: rooms[roomId].status,
            players: rooms[roomId].players,
            minPlayers: MIN_PLAYERS,
            maxPlayers: MAX_PLAYERS,
            chatMessages: rooms[roomId].chatMessages || []
        });

        if (type === 'public') {
            io.emit('rooms_list', publicRoomsList());
        }
    });

    socket.on('join_by_code', ({ code, nickname }) => {
        const cleanCode = code.trim().toUpperCase();
        const targetRoom = Object.values(rooms).find(r => r.code === cleanCode);

        if (targetRoom) {
            // Guard against duplicates: if the socket is already in the room (e.g. a
            // repeated click), don't add it a second time
            const alreadyInRoom = targetRoom.players.some(p => p.id === socket.id);

            if (!alreadyInRoom) {
                if (targetRoom.status !== 'open') {
                    console.log(`join_by_code REJECTED: room ${targetRoom.code} is locked (status=${targetRoom.status})`);
                    socket.emit('join_error', 'This HQ is currently preparing to launch. Try again shortly.');
                    return;
                }

                if (targetRoom.players.length >= MAX_PLAYERS) {
                    console.log(`join_by_code REJECTED: room ${targetRoom.code} is full (${targetRoom.players.length}/${MAX_PLAYERS})`);
                    socket.emit('join_error', `Room is full (${MAX_PLAYERS}/${MAX_PLAYERS} agents).`);
                    return;
                }

                targetRoom.players.push({ id: socket.id, nickname: nickname || 'Agent', character: null, isReady: false, isEliminated: false, isObserver: false, inventory: [] });
            }

            socket.join(targetRoom.id);

            console.log(`Socket ${socket.id} joined room ${targetRoom.id} (${targetRoom.code})`);
            logRoomState('join_by_code', targetRoom);

            socket.emit('room_joined', {
                roomId: targetRoom.id,
                roomCode: targetRoom.code,
                roomName: targetRoom.name,
                type: targetRoom.type,
                hostId: targetRoom.hostId,
                status: targetRoom.status,
                players: targetRoom.players,
                minPlayers: MIN_PLAYERS,
                maxPlayers: MAX_PLAYERS,
                chatMessages: targetRoom.chatMessages || []
            });

            io.to(targetRoom.id).emit('room_updated', targetRoom);

            if (targetRoom.type === 'public') {
                io.emit('rooms_list', publicRoomsList());
            }
        } else {
            socket.emit('join_error', 'Room not found. Check the code!');
        }
    });

    socket.on('select_character', ({ code, character }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) {
            console.log('select_character: room not found for code', code);
            return;
        }

        const player = targetRoom.players.find(p => p.id === socket.id);

        if (player && !player.isReady) {
            const isCharTaken = targetRoom.players.some(p => p.id !== socket.id && p.character === character);
            if (isCharTaken) return;

            player.character = character;

            logRoomState('select_character', targetRoom);
            io.to(targetRoom.id).emit('room_updated', targetRoom);
        }
    });

    // Host clicks "START OPERATION": lock the room from new joins and move it
    // into preparation mode — only now can players toggle READY.
    socket.on('start_preparation', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) {
            console.log('start_preparation: room not found for code', code);
            return;
        }

        if (targetRoom.hostId !== socket.id) {
            console.log('start_preparation REJECTED: socket is not host', socket.id, 'expected', targetRoom.hostId);
            return;
        }

        if (targetRoom.status !== 'open') {
            console.log('start_preparation REJECTED: room already preparing/locked', targetRoom.code);
            return;
        }

        if (targetRoom.players.length < MIN_PLAYERS || targetRoom.players.length > MAX_PLAYERS) {
            console.log('start_preparation REJECTED: player count outside allowed range', targetRoom.players.length, `${MIN_PLAYERS}-${MAX_PLAYERS}`);
            return;
        }

        targetRoom.status = 'preparing';
        targetRoom.players.forEach(p => { p.isReady = false; });

        console.log(`Room ${targetRoom.code} entering PREPARING mode (host: ${socket.id}) — join locked`);
        logRoomState('start_preparation', targetRoom);

        io.to(targetRoom.id).emit('room_updated', targetRoom);
        io.emit('rooms_list', publicRoomsList()); // room disappears from the public list
    });

    socket.on('toggle_ready', ({ code, isReady, character }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) {
            console.log('toggle_ready: room not found for code', code);
            return;
        }

        // Ready state can only be toggled during preparation (after the host starts it)
        if (targetRoom.status !== 'preparing') {
            console.log('toggle_ready IGNORED: room is not in preparing mode', targetRoom.code, 'status=', targetRoom.status);
            return;
        }

        const player = targetRoom.players.find(p => p.id === socket.id);
        if (!player) {
            console.log('toggle_ready: player not found for socket', socket.id, 'in room', targetRoom.code);
            return;
        }

        if (!player.character && character) {
            player.character = character;
        }

        player.isReady = isReady;

        console.log(`toggle_ready: socket=${socket.id} nick=${player.nickname} isReady=${isReady}`);
        logRoomState('toggle_ready BEFORE emit', targetRoom);

        io.to(targetRoom.id).emit('room_updated', targetRoom);

        const allReady = targetRoom.players.length >= MIN_PLAYERS && targetRoom.players.length <= MAX_PLAYERS && targetRoom.players.every(p => p.isReady);

        if (allReady) {
            startCountdown(targetRoom);
        } else {
            cancelCountdown(targetRoom);
        }
    });

    // Lets a client measure the gap between its own system clock and the
    // server's. Every countdown in this game (turn, trial, etc.) is sent to
    // clients as an ABSOLUTE endsAt timestamp, and each client computes its
    // own "remaining = endsAt - Date.now()" locally every tick. If a player's
    // system clock is off from real time (wrong timezone, unsynced clock,
    // VPN, etc.) that skew leaks straight into every one of those countdowns
    // as extra or missing seconds — even though the server's own timers
    // (setTimeout/setInterval) are completely accurate and unaffected.
    // The client is expected to call this once on connect and periodically
    // afterward, then add the resulting offset to its own Date.now() before
    // comparing against any endsAt value. See serverTimeOffset on the client.
    socket.on('time_sync', () => {
        socket.emit('time_sync_response', { serverTime: Date.now() });
    });

    // Client has loaded the black game screen after the fade-out. Once ALL players
    // in the room have loaded, roles are assigned and the intro text starts in
    // sync for everyone.
    socket.on('game_loaded', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) {
            console.log('game_loaded: room not found for code', code);
            return;
        }

        if (!targetRoom.loadedPlayers) targetRoom.loadedPlayers = [];
        if (!targetRoom.loadedPlayers.includes(socket.id)) {
            targetRoom.loadedPlayers.push(socket.id);
        }

        console.log(`game_loaded: room=${targetRoom.code} loaded=${targetRoom.loadedPlayers.length}/${targetRoom.players.length}`);

        // Start a safety timer on the FIRST game_loaded of this run. If not
        // everyone loads within LOAD_TIMEOUT_MS (e.g. someone's tab is
        // backgrounded and its timers are throttled), roles are force-assigned
        // and the intro is force-started for the current player list anyway —
        // otherwise the whole room would hang on "Decrypting identity..."
        // forever, since assignRoles would never be called.
        if (!targetRoom.introStarted && !roomLoadTimers[targetRoom.id]) {
            roomLoadTimers[targetRoom.id] = setTimeout(() => {
                delete roomLoadTimers[targetRoom.id];
                if (!targetRoom.introStarted) {
                    console.log(`LOAD TIMEOUT: forcing intro_start for room ${targetRoom.code} (${targetRoom.loadedPlayers.length}/${targetRoom.players.length} loaded)`);
                    targetRoom.introStarted = true;
                    assignRoles(targetRoom);
                    // Re-send the room roster now that the game has actually started.
                    // Without this, the client's activeRoom (and therefore
                    // activeRoom.players, used e.g. by AccompliceChangeEvidenceModal)
                    // stays null from the moment the game screen mounted until the
                    // next incidental 'room_updated' (normally not until the first
                    // trial resolves) — so any player-list UI reads an empty array
                    // for the whole first round.
                    io.to(targetRoom.id).emit('room_updated', roomUpdatedPayload(targetRoom));
                    io.to(targetRoom.id).emit('intro_start', { code: targetRoom.code });
                }
            }, LOAD_TIMEOUT_MS);
        }

        if (targetRoom.loadedPlayers.length >= targetRoom.players.length && !targetRoom.introStarted) {
            if (roomLoadTimers[targetRoom.id]) {
                clearTimeout(roomLoadTimers[targetRoom.id]);
                delete roomLoadTimers[targetRoom.id];
            }
            targetRoom.introStarted = true;
            assignRoles(targetRoom);
            // Same reasoning as the LOAD TIMEOUT branch above: refresh the client's
            // room roster right as the game starts, otherwise activeRoom.players
            // stays empty for the whole first round (see AccompliceChangeEvidenceModal).
            io.to(targetRoom.id).emit('room_updated', roomUpdatedPayload(targetRoom));
            io.to(targetRoom.id).emit('intro_start', { code: targetRoom.code });
            console.log(`intro_start emitted for room ${targetRoom.code}`);
        }
    });

    // Vote to skip the intro text. Only skips once EVERY player in the room has voted.
    socket.on('vote_skip_intro', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) {
            console.log('vote_skip_intro: room not found for code', code);
            return;
        }

        if (!targetRoom.skipVotes) targetRoom.skipVotes = [];
        if (!targetRoom.skipVotes.includes(socket.id)) {
            targetRoom.skipVotes.push(socket.id);
        }

        const count = targetRoom.skipVotes.length;
        const total = targetRoom.players.length;

        io.to(targetRoom.id).emit('skip_intro_update', { code: targetRoom.code, count, total });
        console.log(`vote_skip_intro: room=${targetRoom.code} ${count}/${total}`);

        if (count >= total) {
            io.to(targetRoom.id).emit('intro_skip', { code: targetRoom.code });
            console.log(`intro_skip emitted for room ${targetRoom.code}`);
        }
    });

    // Client didn't get 'role_assigned' in time (lost packet, render race,
    // reconnect with a new socket.id, etc.) and is asking for its role again. If
    // roles haven't been assigned yet (assignRoles hasn't run, e.g. it's too
    // early), just log it — the client will retry.
    socket.on('request_role', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) {
            console.log('request_role: room not found for code', code);
            return;
        }

        const assignedRole = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;

        if (assignedRole) {
            console.log(`request_role: resending role to ${socket.id} in room ${targetRoom.code}: ${assignedRole}`);
            socket.emit('role_assigned', { role: assignedRole });
        } else {
            console.log(`request_role: no role found yet for ${socket.id} in room ${targetRoom.code} (roles not assigned yet?)`);
        }
    });

    // Client has finished watching its role reveal (5s display + 1s fade locally)
    // and is ready for the game itself to start. Once ALL players in the room have
    // confirmed, the game and first round start for everyone at once. Same as
    // 'game_loaded', there's a safety timeout: if someone gets stuck (e.g. a
    // backgrounded tab throttling its timers), the game still starts after
    // LOAD_TIMEOUT_MS so the room doesn't hang on the role screen forever.
    socket.on('role_reveal_done', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) {
            console.log('role_reveal_done: room not found for code', code);
            return;
        }

        if (targetRoom.game) return; // game already started, ignore a repeat signal

        if (!targetRoom.roleRevealConfirmed) targetRoom.roleRevealConfirmed = [];
        if (!targetRoom.roleRevealConfirmed.includes(socket.id)) {
            targetRoom.roleRevealConfirmed.push(socket.id);
        }

        console.log(`role_reveal_done: room=${targetRoom.code} confirmed=${targetRoom.roleRevealConfirmed.length}/${targetRoom.players.length}`);

        const roleTimerKey = `${targetRoom.id}_role`;
        if (!roomLoadTimers[roleTimerKey]) {
            roomLoadTimers[roleTimerKey] = setTimeout(() => {
                delete roomLoadTimers[roleTimerKey];
                if (!targetRoom.game) {
                    console.log(`ROLE REVEAL TIMEOUT: force-starting the game for room ${targetRoom.code} (${targetRoom.roleRevealConfirmed.length}/${targetRoom.players.length} confirmed)`);
                    startGame(targetRoom);
                }
            }, LOAD_TIMEOUT_MS);
        }

        if (targetRoom.roleRevealConfirmed.length >= targetRoom.players.length) {
            if (roomLoadTimers[roleTimerKey]) {
                clearTimeout(roomLoadTimers[roleTimerKey]);
                delete roomLoadTimers[roleTimerKey];
            }
            startGame(targetRoom);
        }
    });

    // During their turn, a player picks a mansion room to search (map + fog of
    // war). This IS the player's one action for the turn: their character is
    // considered to have physically moved into that room. Exactly ONE search is
    // allowed per turn. In response, the room's contents are sent PRIVATELY to
    // this player only (socket.emit, not a broadcast): which other players have
    // already checked in there this round. No other player receives anything —
    // their map stays fogged until it's their turn.
    //
    // Once the room is confirmed, the client remains inside the peek view until
    // it explicitly ends the turn or the full turn timer expires. The server does
    // not auto-advance here anymore — the turn stays active while the player inspects.
    socket.on('select_room', ({ code, roomId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('select_room IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player) {
            console.log('select_room REJECTED: player not found', socket.id);
            return;
        }
        const isObserverMode = Boolean(player?.isObserver || player?.isEliminated);
        const currentPlayerId = game.turnOrder[game.currentTurnIndex];
        if (!isObserverMode && currentPlayerId !== socket.id) {
            console.log(`select_room REJECTED: ${socket.id} is not the current player (expected ${currentPlayerId})`);
            return;
        }

        if (game.pendingKillDecision && game.pendingKillDecision.killerId === socket.id) {
            console.log(`select_room REJECTED: ${socket.id} has a pending post-kill decision`);
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell — a player confined
        // there this round can't move themselves elsewhere either.
        if (!isObserverMode && isConfinedToHoldingCell(game, socket.id)) {
            console.log(`select_room REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            return;
        }

        if (!game.playerLocations) game.playerLocations = {};
        if (!game.roomOccupants) game.roomOccupants = {};

        if (!isObserverMode && game.playerLocations[socket.id]) {
            console.log(`select_room REJECTED: ${socket.id} already searched a room this turn`);
            return;
        }

        const roomInfo = findMansionRoomById(roomId);
        if (!roomInfo) {
            console.log('select_room: unknown room', roomId);
            return;
        }
        if (roomId === 'f1_holding_cell' && !isObserverMode) {
            console.log('select_room REJECTED: holding cell is spectator-only', socket.id);
            return;
        }

        // Spectators freely inspect rooms, but never enter the authoritative
        // occupancy/location state and therefore cannot affect active gameplay.
        if (isObserverMode) {
            if (!game.spectatorViews) game.spectatorViews = {};
            game.spectatorViews[socket.id] = roomId;
            socket.emit('room_entered', {
                roomId: roomInfo.id,
                roomName: roomInfo.name,
                floor: roomInfo.floor,
                occupants: activeRoomOccupants(targetRoom, roomId),
                bodies: exposedBodiesForRoom(targetRoom, roomId),
                spectator: true
            });
            return;
        }

        const occupantIds = game.roomOccupants[roomId] || [];
        const occupants = activeRoomOccupants(targetRoom, roomId).filter(occupant => occupant.nickname !== player.nickname);

        // The player's character is now physically located in this room.
        game.playerLocations[socket.id] = roomId;
        game.roomOccupants[roomId] = occupantIds.includes(socket.id) ? occupantIds : [...occupantIds, socket.id];
        // Walking into a trapped room (see 'set_trap') pops a warning to this
        // player and consumes the trap — see triggerTrapIfPresent.
        triggerTrapIfPresent(targetRoom, roomId, player);
        // A fresh room re-arms the turn's room-interaction phase — see
        // 'search_body' / 'investigate_room'.
        if (!game.roomActionUsedThisTurn) game.roomActionUsedThisTurn = {};
        game.roomActionUsedThisTurn[socket.id] = false;
        // A fresh room also re-locks 'check_room' until this room specifically
        // gets investigated again — see the field comment in startGame.
        if (!game.investigateRoomUsedThisTurn) game.investigateRoomUsedThisTurn = {};
        game.investigateRoomUsedThisTurn[socket.id] = false;
        // A body left exposed in this room is "stumbled onto" the instant
        // someone walks in — same rule the CLUES board already applies to
        // evidence, just for bodies (see creditExposedBodyDiscovery). This is
        // what lets 'submit_innocent_code' eventually unlock: the case isn't
        // closed on a body until someone has actually laid eyes on it.
        if (creditExposedBodyDiscovery(targetRoom, roomId, player, game.round)) {
            broadcastExitStatus(targetRoom);
        }
        emitSpectatorRoomUpdates(targetRoom, roomId);

        console.log(`select_room: player ${socket.id} moved into "${roomInfo.name}" (floor ${roomInfo.floor}) in room ${targetRoom.code} — found ${occupants.length} other agent(s) there`);

        socket.emit('room_entered', {
            roomId: roomInfo.id,
            roomName: roomInfo.name,
            floor: roomInfo.floor,
            occupants,
            // Exposed bodies (isHidden: false) are detectable just by walking in —
            // no explicit 'search_body' needed, unlike a hidden one (see
            // exposedBodiesForRoom / 'resolve_kill').
            bodies: exposedBodiesForRoom(targetRoom, roomId),
            playerLocations: { ...game.playerLocations },
            inspectMs: ROOM_INSPECT_MS
            // Neurotoxin-7 is deliberately NOT surfaced here — same as
            // evidence, it only ever shows up once the room is actually
            // investigated (see 'investigate_room'), not just walked into.
        });

        // The turn stays active until the player explicitly ends it or the main
        // turn timer expires. We still record the selected room so the client can
        // render the peek view, but we do not start a shortened auto-end timer.
        console.log(`Room ${targetRoom.code}: player ${currentPlayerId} peeked into "${roomInfo.name}"`);
    });

    // Killer-only: instantly relocates from one of six vented rooms to its
    // paired destination (see VENTS). Requires the killer to already be
    // standing in a vent room this turn — i.e. they must have 'select_room'ed
    // into it first — so this is a bonus reposition on top of their search,
    // not a replacement for it. Exactly one vent hop is allowed per turn: once
    // used, the flag below blocks BOTH climbing back through the same vent
    // and chaining into a different one for the rest of the turn.
    socket.on('use_vent', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('use_vent IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('use_vent REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const currentPlayerId = game.turnOrder[game.currentTurnIndex];
        if (currentPlayerId !== socket.id) {
            console.log(`use_vent REJECTED: ${socket.id} is not the current player (expected ${currentPlayerId})`);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Killer') {
            console.log(`use_vent REJECTED: ${socket.id} is role=${role || 'unknown'}, not Killer`);
            return;
        }

        if (game.pendingKillDecision && game.pendingKillDecision.killerId === socket.id) {
            console.log(`use_vent REJECTED: ${socket.id} has a pending post-kill decision`);
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`use_vent REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): "vent" is the Killer's
        // active ability, not plain movement — blocked for the entire round
        // after tripping a trap, same as every other ability below.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'use_vent');
            return;
        }

        if (!game.playerLocations) game.playerLocations = {};
        if (!game.roomOccupants) game.roomOccupants = {};
        if (!game.ventUsedThisTurn) game.ventUsedThisTurn = {};

        if (game.ventUsedThisTurn[socket.id]) {
            console.log(`use_vent REJECTED: ${socket.id} already used a vent this turn`);
            return;
        }

        const currentRoomId = game.playerLocations[socket.id];
        const destinationRoomId = currentRoomId ? VENTS[currentRoomId] : null;
        if (!currentRoomId || !destinationRoomId) {
            console.log(`use_vent REJECTED: ${socket.id} is not standing in a vented room (current=${currentRoomId || 'none'})`);
            return;
        }

        const roomInfo = findMansionRoomById(destinationRoomId);
        if (!roomInfo) {
            console.log('use_vent: unknown destination room', destinationRoomId);
            return;
        }

        // Pull the killer out of the source room's occupant ledger and drop
        // them into the destination's — playerLocations alone isn't enough,
        // roomOccupants is what drives what OTHER players see when they
        // later search either room this round.
        if (game.roomOccupants[currentRoomId]) {
            game.roomOccupants[currentRoomId] = game.roomOccupants[currentRoomId].filter(id => id !== socket.id);
        }
        const destOccupantIds = game.roomOccupants[destinationRoomId] || [];
        game.roomOccupants[destinationRoomId] = destOccupantIds.includes(socket.id) ? destOccupantIds : [...destOccupantIds, socket.id];

        game.playerLocations[socket.id] = destinationRoomId;
        game.ventUsedThisTurn[socket.id] = true;
        // Walking into a trapped room via vent counts the same as walking in
        // through 'select_room' — see triggerTrapIfPresent.
        triggerTrapIfPresent(targetRoom, destinationRoomId, player);
        // Venting counts as entering a fresh room, same as 'select_room' — the
        // player gets a brand-new room-interaction phase in the destination.
        if (!game.roomActionUsedThisTurn) game.roomActionUsedThisTurn = {};
        game.roomActionUsedThisTurn[socket.id] = false;
        // Same re-lock as 'select_room' — see the field comment in startGame.
        if (!game.investigateRoomUsedThisTurn) game.investigateRoomUsedThisTurn = {};
        game.investigateRoomUsedThisTurn[socket.id] = false;

        if (creditExposedBodyDiscovery(targetRoom, destinationRoomId, player, game.round)) {
            broadcastExitStatus(targetRoom);
        }
        emitSpectatorRoomUpdates(targetRoom, currentRoomId);
        emitSpectatorRoomUpdates(targetRoom, destinationRoomId);

        const occupants = activeRoomOccupants(targetRoom, destinationRoomId).filter(occupant => occupant.nickname !== player.nickname);

        console.log(`use_vent: player ${socket.id} vented from "${currentRoomId}" into "${roomInfo.name}" (floor ${roomInfo.floor}) in room ${targetRoom.code}`);

        // Reuses the exact 'room_entered' payload shape 'select_room' sends —
        // the client's existing handler already knows how to redraw the peek
        // view from this, no separate event needed. `viaVent` just lets the
        // client know not to re-trigger vent-arrival side effects (sounds,
        // toasts) as if it were a fresh search.
        socket.emit('room_entered', {
            roomId: roomInfo.id,
            roomName: roomInfo.name,
            floor: roomInfo.floor,
            occupants,
            bodies: exposedBodiesForRoom(targetRoom, destinationRoomId),
            playerLocations: { ...game.playerLocations },
            viaVent: true
        });
    });

    // Killer-only: eliminates another active player standing in the SAME
    // mansion room this turn. This is the kill itself — the victim is
    // eliminated the instant it lands. It does NOT by itself decide what
    // happens to the body left behind; that's a separate, mandatory follow-up
    // (see 'resolve_kill') the Killer must resolve before doing anything else
    // this turn (every other action handler below checks
    // game.pendingKillDecision and rejects while one is outstanding).
    socket.on('kill_player', ({ code, targetId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('kill_player IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const killer = targetRoom.players.find(p => p.id === socket.id);
        if (!killer || killer.isEliminated || killer.isObserver) {
            console.log('kill_player REJECTED: killer eliminated, observing, or not found', socket.id);
            return;
        }

        const currentPlayerId = game.turnOrder[game.currentTurnIndex];
        if (currentPlayerId !== socket.id) {
            console.log(`kill_player REJECTED: ${socket.id} is not the current player (expected ${currentPlayerId})`);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Killer') {
            console.log(`kill_player REJECTED: ${socket.id} is role=${role || 'unknown'}, not Killer`);
            return;
        }

        if (game.pendingKillDecision) {
            console.log(`kill_player REJECTED: ${socket.id} already has a pending post-kill decision`);
            return;
        }

        // Normally a Killer gets exactly 1 kill per turn. Carrying an
        // unconsumed Neurotoxin-7 raises that to 2 within the same round
        // (this game gives a Killer only one turn per round, so "this
        // round" and "this turn" are the same window here).
        const killsSoFarThisTurn = game.killUsedThisTurn?.[socket.id] || 0;
        if (killsSoFarThisTurn >= killerMaxKillsThisTurn(killer)) {
            console.log(`kill_player REJECTED: ${socket.id} already used their kill(s) this turn`);
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`kill_player REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): the Killer's kill ability
        // is off the table for their entire penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'kill_player');
            return;
        }

        if (!game.playerLocations) game.playerLocations = {};
        const killerRoomId = game.playerLocations[socket.id];
        if (!killerRoomId) {
            console.log(`kill_player REJECTED: ${socket.id} has not entered a room this turn yet`);
            return;
        }

        const target = targetRoom.players.find(p => p.id === targetId);
        if (!target || target.isEliminated || target.isObserver || target.id === socket.id) {
            console.log('kill_player REJECTED: invalid or already-eliminated target', targetId);
            return;
        }

        // Trust server-authoritative locations for both parties — never just
        // whatever room id the client happens to send.
        if (game.playerLocations[targetId] !== killerRoomId) {
            console.log(`kill_player REJECTED: target ${targetId} is not in the same room as killer ${socket.id}`);
            return;
        }

        // --- NEUROTOXIN-7: passive shield check (Accomplice/Joker) --------
        // If the target is carrying an unconsumed Neurotoxin-7, the attack
        // is negated here: no elimination, no body, no pending decision.
        // Only the Killer is told why (the shield trigger itself is
        // reported privately to the victim inside tryNeurotoxinShield).
        //
        // A failed/shielded attempt costs the Killer their ENTIRE round —
        // not just a single kill charge. So instead of bumping the counter
        // by 1 (which, with the Killer's own unconsumed Neurotoxin-7, would
        // still leave a second attempt available this turn), the counter is
        // pinned to a value no killerMaxKillsThisTurn() can ever clear,
        // permanently failing the `killsSoFarThisTurn >= ...` gate above for
        // the rest of this round.
        if (tryNeurotoxinShield(targetRoom, target)) {
            if (!game.killUsedThisTurn) game.killUsedThisTurn = {};
            game.killUsedThisTurn[socket.id] = Infinity;
            console.log(`kill_player: room=${targetRoom.code} KILLER ${socket.id} attack on ${target.nickname} was NEGATED by a Neurotoxin-7 shield — killer locked out of killing for the rest of this round`);
            socket.emit('kill_options', {
                code: targetRoom.code,
                negatedByShield: true,
                targetId,
                targetNickname: target.nickname
            });
            return;
        }

        // The kill lands immediately.
        target.isEliminated = true;
        target.isObserver = true;

        if (!game.killUsedThisTurn) game.killUsedThisTurn = {};
        game.killUsedThisTurn[socket.id] = killsSoFarThisTurn + 1;

        // --- NEUROTOXIN-7: Killer-side effect ------------------------------
        // Tracks the double-kill window and consumes the item once 2 kills
        // have landed within this round (see also startNewRound, which
        // resets this counter for a Killer who only got 1 kill last round).
        let neurotoxinMessage = null;
        const neurotoxinEntry = findNeurotoxinEntry(killer);
        if (neurotoxinEntry) {
            neurotoxinEntry.killsInCurrentRound += 1;
            if (neurotoxinEntry.killsInCurrentRound >= NEUROTOXIN_REQUIRED_KILLS) {
                removeNeurotoxinEntry(killer);
                neurotoxinMessage = NEUROTOXIN_MESSAGES.doubleKill;
                console.log(`kill_player: room=${targetRoom.code} KILLER ${socket.id} double kill this round — Neurotoxin-7 consumed`);
            } else {
                neurotoxinMessage = NEUROTOXIN_MESSAGES.firstKill;
            }
        }

        // Drop the victim from the raw occupancy ledger too — activeRoomOccupants
        // already filters eliminated players out, but a stale entry here could
        // still confuse a later vent-hop's bookkeeping in this same room.
        if (!game.roomOccupants) game.roomOccupants = {};
        if (game.roomOccupants[killerRoomId]) {
            game.roomOccupants[killerRoomId] = game.roomOccupants[killerRoomId].filter(id => id !== targetId);
        }

        game.pendingKillDecision = {
            killerId: socket.id,
            targetId,
            targetNickname: target.nickname,
            targetCharacter: target.character,
            roomId: killerRoomId
        };

        console.log(`kill_player: room=${targetRoom.code} KILLER ${socket.id} eliminated ${target.nickname} in "${killerRoomId}"`);

        // Broadcast the elimination immediately — every other isEliminated/
        // isObserver-driven bit of UI (trial eligibility, chat locks, etc.)
        // reacts off this the same way it already does for a council execution.
        io.to(targetRoom.id).emit('room_updated', roomUpdatedPayload(targetRoom));
        io.to(targetRoom.id).emit('player_eliminated', {
            code: targetRoom.code,
            targetId,
            nickname: target.nickname,
            reason: 'killed'
        });
        emitSpectatorRoomUpdates(targetRoom, killerRoomId);

        // Win condition #1: this kill may have just brought the number of
        // surviving peaceful players down to (or below) the number of
        // remaining Killer-team players. If so, the match is already over —
        // skip the body-disposal follow-up (resolve_kill) entirely, since
        // there's nothing left to play for.
        if (checkKillerMajority(targetRoom)) {
            return;
        }

        // Only the Killer themself is told a decision is pending — nobody else
        // needs to know this step even exists.
        socket.emit('kill_options', {
            code: targetRoom.code,
            targetId,
            targetNickname: target.nickname,
            targetCharacter: target.character,
            roomId: killerRoomId,
            neurotoxinMessage
        });
    });

    // The Killer's mandatory follow-up to 'kill_player': what happens to the
    // body they just left behind. Exactly one of two outcomes:
    //   - 'hide'   -> body concealed (ONLY surfaces via an explicit 'search_body'),
    //                 but this spends the Killer's one vent hop for the turn —
    //                 hiding the body means staying put, no vent travel this turn.
    //   - 'expose' -> body left exposed where anyone entering the room will see
    //                 it. The Killer stays put too, but is still free to use
    //                 the room's vent afterward via a normal, separate
    //                 'use_vent' call (there's no dedicated instant-escape
    //                 option here anymore — a player can always vent on their
    //                 own whenever it's actually available).
    socket.on('resolve_kill', ({ code, action }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game) {
            console.log('resolve_kill IGNORED: no active game for room', code);
            return;
        }

        const game = targetRoom.game;
        const pending = game.pendingKillDecision;
        if (!pending || pending.killerId !== socket.id) {
            console.log(`resolve_kill REJECTED: ${socket.id} has no pending kill decision`);
            return;
        }

        if (!['hide', 'expose'].includes(action)) {
            console.log(`resolve_kill REJECTED: unknown action "${action}"`);
            return;
        }

        if (!targetRoom.bodies) targetRoom.bodies = [];
        const killerPlayer = targetRoom.players.find(p => p.id === socket.id);
        targetRoom.bodies.push({
            playerId: pending.targetId,
            nickname: pending.targetNickname,
            character: pending.targetCharacter || null,
            roomId: pending.roomId,
            round: game.round,
            isHidden: action === 'hide',
            foundBy: [],
            forensicClue: killerPlayer ? generateForensicClue(targetRoom, killerPlayer.character) : null
        });

        // Hiding the body costs this turn's vent hop — a hidden body and a
        // vent escape are mutually exclusive, so mark the vent as already
        // used the same way an actual 'use_vent' hop would.
        if (action === 'hide') {
            if (!game.ventUsedThisTurn) game.ventUsedThisTurn = {};
            game.ventUsedThisTurn[socket.id] = true;
        }

        // --- KILLER'S ACCIDENTAL TRACE ---------------------------------------
        // On every kill, there's a flat 50% chance the Killer accidentally
        // leaves behind one of their OWN character's 3 personal items (see
        // CHARACTER_EVIDENCE) — same shape/mechanism as the Joker's deliberate
        // plant_joker_evidence, just automatic and random instead of a chosen
        // action. It's dropped into a random searchable mansion room (any
        // room except the spectator-only f1_holding_cell — see
        // allSearchableRoomIds), NOT necessarily the murder room itself, so it
        // can't be used to instantly pinpoint the kill location. Same as
        // Joker evidence, the server never states who left it — only what was
        // found, wherever it's found (see plantedEvidenceForRoom / CLUES
        // board). Only the Killer themself is told this happened, via the
        // `killerClue` field on 'kill_resolved' below.
        let killerClue = null;
        if (killerPlayer && Math.random() < 0.5) {
            const evidencePool = CHARACTER_EVIDENCE[killerPlayer.character] || [{ name: 'Mysterious personal item', description: 'An unidentified item with no further clues.' }];
            const clueData = evidencePool[Math.floor(Math.random() * evidencePool.length)];
            const dropCandidates = allSearchableRoomIds();
            const dropRoomId = dropCandidates[Math.floor(Math.random() * dropCandidates.length)];
            const dropRoomInfo = findMansionRoomById(dropRoomId);

            const clueEntry = {
                id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
                text: clueData.name,
                description: clueData.description,
                roomId: dropRoomId,
                roomName: dropRoomInfo?.name || dropRoomId,
                round: game.round,
                plantedAt: Date.now(),
                // Genuinely left behind by the actual Killer — the one case
                // 'verify_evidence' (Forensic Examiner) reports as AUTHENTIC.
                isPlanted: false
            };

            if (!targetRoom.plantedEvidence) targetRoom.plantedEvidence = {};
            if (!targetRoom.plantedEvidence[dropRoomId]) targetRoom.plantedEvidence[dropRoomId] = [];
            targetRoom.plantedEvidence[dropRoomId].push(clueEntry);

            killerClue = { text: clueData.name, roomId: dropRoomId, roomName: dropRoomInfo?.name || dropRoomId };

            console.log(`resolve_kill: room=${targetRoom.code} KILLER ${socket.id} accidentally left behind "${clueData.name}" in "${dropRoomInfo?.name || dropRoomId}"`);

            // Someone might already be peeking that room as a spectator — let
            // their view refresh immediately, same as a Joker's deliberate plant.
            emitSpectatorRoomUpdates(targetRoom, dropRoomId);

            // Let the Accomplice know their Killer slipped up too — same
            // "only the people who need to know" privacy rule as killerClue
            // itself: nobody outside the Killer/Accomplice pair learns this.
            const accompliceId = findRoleHolderId(targetRoom, 'Accomplice');
            if (accompliceId && accompliceId !== socket.id) {
                io.to(accompliceId).emit('accomplice_killer_clue_notice', {
                    code: targetRoom.code,
                    roomId: dropRoomId,
                    roomName: dropRoomInfo?.name || dropRoomId,
                    text: clueData.name
                });
            }
        }

        console.log(`resolve_kill: room=${targetRoom.code} KILLER ${socket.id} chose "${action}" for ${pending.targetNickname}'s body in "${pending.roomId}"`);

        delete game.pendingKillDecision;
        emitSpectatorRoomUpdates(targetRoom, pending.roomId);
        broadcastExitStatus(targetRoom);

        // `killerClue` is only ever included on the Killer's own socket emit
        // below — nobody else's client ever learns whether/where a clue was
        // left behind.
        socket.emit('kill_resolved', { code: targetRoom.code, action, roomId: pending.roomId, targetId: pending.targetId, killerClue });
    });

    // Player investigates the room they've already entered this turn (see
    // 'select_room'), looking for a piece of the digital evidence code. What
    // they actually learn is strictly role-gated on the server:
    //   - Innocents are the only role that ever gets a real digit back.
    //   - Every other role gets the exact same generic "junk" response, whether
    //     or not a real fragment is planted in that room — this is what stops a
    //     non-Innocent from using this action to fish for information about
    //     where the code digits are hidden.
    //   - A room with no fragment at all returns the same "nothing found"
    //     result for everyone, regardless of role.
    socket.on('investigate_room', ({ code, roomId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('investigate_room IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player) {
            console.log('investigate_room REJECTED: player not found', socket.id);
            return;
        }

        const isObserverMode = Boolean(player?.isObserver || player?.isEliminated);

        // Spectators are only watching a peeked room, not playing — they never
        // get an evidence result of their own.
        if (isObserverMode) {
            console.log('investigate_room IGNORED: spectator cannot investigate', socket.id);
            return;
        }

        // Trust server-authoritative state for where the player actually is,
        // never the roomId the client happens to send.
        const actualRoomId = game.playerLocations?.[socket.id];
        if (!actualRoomId || actualRoomId !== roomId) {
            console.log(`investigate_room REJECTED: ${socket.id} is not currently standing in room ${roomId}`);
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`investigate_room REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): no investigating during the
        // penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'investigate_room');
            return;
        }

        if (game.pendingKillDecision && game.pendingKillDecision.killerId === socket.id) {
            console.log(`investigate_room REJECTED: ${socket.id} has a pending post-kill decision`);
            return;
        }

        // "Search for Body" and "Investigate Room" share a single room-interaction
        // phase per turn — whichever the player picks first locks out the other
        // until they enter a fresh room (see 'select_room' / 'use_vent').
        if (!game.roomActionUsedThisTurn) game.roomActionUsedThisTurn = {};
        if (game.roomActionUsedThisTurn[socket.id]) {
            console.log(`investigate_room REJECTED: ${socket.id} already used their room action this turn`);
            return;
        }
        game.roomActionUsedThisTurn[socket.id] = true;
        // Unlocks 'check_room' (Mark Room / "CHECK ROOM" button) for this room —
        // see the field comment in startGame.
        if (!game.investigateRoomUsedThisTurn) game.investigateRoomUsedThisTurn = {};
        game.investigateRoomUsedThisTurn[socket.id] = true;

        const fragment = targetRoom.evidenceLocations?.[roomId];
        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        const totalDigits = (targetRoom.digitalCode || '').length;
        // Any evidence the Joker has planted here (see 'plant_joker_evidence')
        // is revealed to whoever actually investigates the room — not just
        // whoever peeked into it — regardless of their role.
        const evidence = plantedEvidenceForRoom(targetRoom, roomId);

        // Crediting this player as a finder — and broadcasting the shared
        // CLUES board if it changed — is deliberately role-agnostic. Unlike
        // the digital code digit below, physical evidence was never
        // Innocent-only, so everyone who actually investigates a room with
        // evidence in it earns a spot in that clue's "found by" list.
        if (registerClueDiscovery(targetRoom, roomId, player, game.round)) {
            io.to(targetRoom.id).emit('clues_board_update', { code: targetRoom.code, clues: buildCluesBoard(targetRoom) });
        }

        // --- NEUROTOXIN-7: folded into 'investigate_room' ------------------
        // No standalone pickup button any more — if this room still has an
        // unclaimed syringe, investigating it resolves the pickup (or the
        // reason it didn't happen) right here, attached to this same result
        // as `neurotoxin` so the client can pop up a dedicated notification
        // instead of a plain toast. Only ever attached to the actual
        // searcher's own result — never shared with Innocent teammates below,
        // same as planted evidence.
        let neurotoxinResult = null;
        const neurotoxinLocation = findNeurotoxinLocationInRoom(targetRoom, roomId);
        if (neurotoxinLocation) {
            if (!NEUROTOXIN_ELIGIBLE_ROLES.includes(role)) {
                // Wrong role: the syringe is not added to the inventory and
                // stays exactly where it is on the map.
                neurotoxinResult = { outcome: 'restricted_role', message: NEUROTOXIN_MESSAGES.hazardous };
                console.log(`investigate_room: room=${targetRoom.code} role=${role || 'unknown'} ${socket.id} found Neurotoxin-7 in "${roomId}" but cannot touch it`);
            } else if (findNeurotoxinEntry(player)) {
                // Blocker: this player already carries an unconsumed syringe —
                // only one may ever be held at a time, so this one is left
                // exactly where it is for someone else to find later.
                neurotoxinResult = { outcome: 'already_carrying', message: NEUROTOXIN_MESSAGES.alreadyCarrying };
                console.log(`investigate_room: room=${targetRoom.code} ${role} ${socket.id} found a second Neurotoxin-7 in "${roomId}" but is already carrying one — left in place`);
            } else {
                // Eligible role, no syringe currently held: move it from the
                // map into the inventory. It is kept indefinitely across
                // rounds until its full-consumption conditions are met (see
                // 'kill_player' / tryNeurotoxinShield).
                neurotoxinLocation.pickedUp = true;
                ensureInventory(player).push({
                    itemId: NEUROTOXIN_ITEM_ID,
                    definition: NEUROTOXIN_ITEM_DEFINITION,
                    acquiredRound: game.round,
                    // Only meaningful for a Killer; unused for the
                    // Accomplice/Joker passive-shield behavior.
                    killsInCurrentRound: 0
                });
                const message = role === 'Killer' ? NEUROTOXIN_MESSAGES.pickedUpKiller : NEUROTOXIN_MESSAGES.pickedUpShield;
                neurotoxinResult = { outcome: 'picked_up', message, effect: role === 'Killer' ? 'double_kill' : 'shield' };
                console.log(`investigate_room: room=${targetRoom.code} ${role} ${socket.id} picked up Neurotoxin-7 in "${roomId}"`);
                // Let everyone else in the room know this syringe is gone from the map.
                socket.to(targetRoom.id).emit('map:item_removed', { code: targetRoom.code, itemId: NEUROTOXIN_ITEM_ID, roomId });
            }
        }

        if (!fragment) {
            socket.emit('investigate_result', { code: targetRoom.code, roomId, type: 'empty', evidence, neurotoxin: neurotoxinResult });
            console.log(`investigate_room: room=${targetRoom.code} player=${socket.id} found nothing in "${roomId}"`);
            return;
        }

        if (role === 'Innocent') {
            // The Innocents share evidence knowledge as a team: whoever finds a
            // fragment, every Innocent (not just the searcher) learns it — sent
            // individually to each Innocent's own socket, so no other role ever
            // sees this broadcast. Planted evidence, however, is NOT team
            // knowledge — it's only attached to the actual searcher's own
            // result, same as for every other role. Neurotoxin-7 can never
            // apply to an Innocent (see NEUROTOXIN_ELIGIBLE_ROLES) so
            // `neurotoxinResult` here is always null, but it's still only
            // ever attached to the searcher's own payload for consistency.
            const payload = {
                code: targetRoom.code,
                roomId,
                type: 'fragment',
                digit: fragment.digit,
                position: fragment.position,
                totalDigits,
                foundBy: player.nickname,
                selfFound: false // per-recipient flag, set correctly below
            };
            let recipientCount = 0;
            targetRoom.players.forEach(p => {
                if (targetRoom.roles?.[p.id] === 'Innocent') {
                    const isSelf = p.id === socket.id;
                    io.to(p.id).emit('investigate_result', { ...payload, selfFound: isSelf, ...(isSelf ? { evidence, neurotoxin: neurotoxinResult } : {}) });
                    recipientCount += 1;
                }
            });
            console.log(`investigate_room: room=${targetRoom.code} INNOCENT ${socket.id} found digit ${fragment.digit} (${fragment.position}/${totalDigits}) in "${roomId}" — shared with ${recipientCount} Innocent(s)`);
        } else {
            // Digit is deliberately withheld — this branch never touches
            // fragment.digit, so no code information can leak to this role.
            socket.emit('investigate_result', { code: targetRoom.code, roomId, type: 'trash', evidence, neurotoxin: neurotoxinResult });
            console.log(`investigate_room: room=${targetRoom.code} role=${role || 'unknown'} ${socket.id} found trash in "${roomId}" (fragment withheld)`);
        }
    });

    // Innocent's "CHECK ROOM" ability (Mark Room): a dedicated, cooldown-gated
    // action separate from 'investigate_room' above — doesn't touch
    // roomActionUsedThisTurn, so it can still be used after SEARCH FOR BODY in
    // the same turn. It DOES, however, require 'investigate_room' to have
    // already been used on this exact room first (see
    // investigateRoomUsedThisTurn) — Check Room is a way to log/share a room
    // you've already investigated, not a free substitute for investigating it.
    // Confirms whether the room the Innocent is currently standing in holds a
    // code fragment, and either way shares with the rest of the Innocent team
    // that the room was checked — anonymously: teammates only ever learn THAT
    // a room was checked (and, separately, whether it's clean, for the green
    // highlight), never WHO checked it.
    socket.on('check_room', ({ code, roomId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('check_room IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('check_room REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Innocent') {
            console.log(`check_room REJECTED: ${socket.id} is role=${role || 'unknown'}, not Innocent`);
            // Deliberately silent beyond this — a non-Innocent shouldn't even
            // have this button available client-side, so no distinguishable
            // reason is sent back, same treatment as officer_lock_player /
            // plant_joker_evidence.
            return;
        }

        // Trust server-authoritative state for where the player actually is,
        // never the roomId the client happens to send — same guard as
        // 'investigate_room'.
        const actualRoomId = game.playerLocations?.[socket.id];
        if (!actualRoomId || actualRoomId !== roomId) {
            console.log(`check_room REJECTED: ${socket.id} is not currently standing in room ${roomId}`);
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`check_room REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): "check room" is off the
        // table for the Innocent's entire penalty round, same as every other
        // ability.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'check_room');
            socket.emit('check_room_result', { code: targetRoom.code, success: false, reason: 'trap_debuff' });
            return;
        }

        if (!isMarkRoomAbilityAvailable(game, socket.id, game.round)) {
            const status = markRoomAbilityStatusPayload(targetRoom, socket.id);
            console.log(`check_room REJECTED: ${socket.id} on cooldown, ${status.turnsRemaining} round(s) remaining`);
            socket.emit('check_room_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                turnsRemaining: status.turnsRemaining
            });
            return;
        }

        if (!game.investigateRoomUsedThisTurn || !game.investigateRoomUsedThisTurn[socket.id]) {
            console.log(`check_room REJECTED: ${socket.id} has not investigated "${roomId}" yet this turn`);
            socket.emit('check_room_result', {
                code: targetRoom.code,
                success: false,
                reason: 'investigate_required'
            });
            return;
        }

        markMarkRoomAbilityUsed(game, socket.id, game.round);
        emitMarkRoomAbilityStatus(targetRoom, socket.id);

        const roomInfo = findMansionRoomById(roomId);
        const fragment = targetRoom.evidenceLocations?.[roomId];
        const cleared = !fragment;

        if (cleared) {
            // Genuinely no code fragment here — remember it as cleared so the
            // green "already checked" highlight stays accurate for the team.
            if (!targetRoom.innocentClearedRooms) targetRoom.innocentClearedRooms = {};
            if (!targetRoom.innocentClearedRooms[roomId]) {
                targetRoom.innocentClearedRooms[roomId] = { round: game.round };
            }
        } else {
            // A real code fragment IS here — never confirm it as clear, and
            // never reveal the digit through this ability. The Innocent still
            // spent their cooldown finding this out, same as a real search.
            console.log(`check_room: room=${targetRoom.code} INNOCENT ${socket.id} checked "${roomId}" — fragment present, NOT marked clear`);
        }

        socket.emit('check_room_result', {
            code: targetRoom.code,
            success: true,
            cleared,
            roomId,
            roomName: roomInfo?.name || roomId,
            turnsRemaining: MARK_ROOM_COOLDOWN_ROUNDS
        });

        // Anonymous team broadcast — every OTHER Innocent learns the room was
        // checked, regardless of whether a code fragment was actually there
        // — but never who checked it (the acting player already knows via
        // their own 'check_room_result' above, so they're deliberately
        // excluded here to avoid a redundant duplicate toast). `cleared` is
        // still included so their client only lights up the green "clean"
        // highlight when the room genuinely holds no fragment — the toast
        // text itself stays the same generic "has been checked" either way,
        // so this never leaks whether a fragment is actually present.
        let recipientCount = 0;
        targetRoom.players.forEach(p => {
            if (p.id === socket.id) return;
            if (targetRoom.roles?.[p.id] === 'Innocent') {
                io.to(p.id).emit('room_marked_clean', {
                    code: targetRoom.code,
                    roomId,
                    roomName: roomInfo?.name || roomId,
                    cleared
                });
                recipientCount += 1;
            }
        });
        console.log(`check_room: room=${targetRoom.code} INNOCENT ${socket.id} checked "${roomId}" (cleared=${cleared}) — shared anonymously with ${recipientCount} other Innocent(s)`);
    });

    // Player checks the room they've already entered this turn (see
    // 'select_room') for a body — a Killer's victim left behind via
    // 'resolve_kill' (council-executed players leave no body, see
    // resolveTrialPhase). Shares the same single room-interaction phase as
    // 'investigate_room': whichever fires first for this turn locks out the
    // other (see the shared 'roomActionUsedThisTurn' flag). A found body
    // stays in the room for the rest of the match — it isn't consumed or
    // removed, so anyone who later searches the same room finds it again.
    socket.on('search_body', ({ code, roomId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('search_body IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player) {
            console.log('search_body REJECTED: player not found', socket.id);
            return;
        }

        const isObserverMode = Boolean(player?.isObserver || player?.isEliminated);

        // Spectators are only watching a peeked room, not playing — they never
        // trigger a body-search result of their own.
        if (isObserverMode) {
            console.log('search_body IGNORED: spectator cannot search for a body', socket.id);
            return;
        }

        // Trust server-authoritative state for where the player actually is,
        // never the roomId the client happens to send.
        const actualRoomId = game.playerLocations?.[socket.id];
        if (!actualRoomId || actualRoomId !== roomId) {
            console.log(`search_body REJECTED: ${socket.id} is not currently standing in room ${roomId}`);
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`search_body REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): no finding bodies during
        // the penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'search_body');
            return;
        }

        if (game.pendingKillDecision && game.pendingKillDecision.killerId === socket.id) {
            console.log(`search_body REJECTED: ${socket.id} has a pending post-kill decision`);
            return;
        }

        // Shared with 'investigate_room' — see that handler for the full
        // explanation of this lock.
        if (!game.roomActionUsedThisTurn) game.roomActionUsedThisTurn = {};
        if (game.roomActionUsedThisTurn[socket.id]) {
            console.log(`search_body REJECTED: ${socket.id} already used their room action this turn`);
            return;
        }
        game.roomActionUsedThisTurn[socket.id] = true;

        const bodiesHere = (targetRoom.bodies || []).filter(b => b.roomId === roomId);
        if (bodiesHere.length === 0) {
            socket.emit('search_body_result', { code: targetRoom.code, roomId, found: false });
            console.log(`search_body: room=${targetRoom.code} player=${socket.id} found no bodies in "${roomId}"`);
            return;
        }

        // Credit this player as a finder on every body present, without
        // duplicating them in a body's foundBy list on repeat searches.
        // Finding a hidden body this way also exposes it for good — mirrors
        // the Killer's "LEAVE BODY EXPOSED" choice in resolve_kill. From this
        // point on it's no longer hidden: anyone who simply walks into the
        // room (or a spectator currently peeking it) sees it too via
        // exposedBodiesForRoom, without needing to search for it themselves.
        let newlyDiscovered = false;
        let newlyExposed = false;
        bodiesHere.forEach(body => {
            if (!body.foundBy) body.foundBy = [];
            if (!body.foundBy.some(f => f.id === socket.id)) {
                body.foundBy.push({ id: socket.id, nickname: player.nickname, round: game.round });
                newlyDiscovered = true;
            }
            if (body.isHidden) {
                body.isHidden = false;
                newlyExposed = true;
            }
        });
        if (newlyDiscovered) broadcastExitStatus(targetRoom);
        if (newlyExposed) emitSpectatorRoomUpdates(targetRoom, roomId);

        socket.emit('search_body_result', {
            code: targetRoom.code,
            roomId,
            found: true,
            bodies: bodiesHere.map(b => {
                const pos = getEntityPosition(targetRoom, roomId, b.playerId || b.nickname);
                return { playerId: b.playerId || null, nickname: b.nickname, round: b.round, character: b.character || null, x: pos.x, y: pos.y };
            })
        });
        console.log(`search_body: room=${targetRoom.code} player=${socket.id} found ${bodiesHere.length} body(ies) in "${roomId}"`);
    });

    // Lets a client pull the current shared CLUES board on demand — used when
    // the CLUES button/panel is first opened, and again on reconnect, so a
    // player never has to wait for the next 'clues_board_update' broadcast to
    // see clues that were already discovered before they asked. Available to
    // anyone in the room, including eliminated players/spectators, same as
    // the trial roster — this is a shared case board, not team-private intel.
    socket.on('get_clues_board', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game) return;
        socket.emit('clues_board_update', { code: targetRoom.code, clues: buildCluesBoard(targetRoom) });
    });

    // Forensic Examiner's signature power — "Verify Evidence Authenticity":
    // checks whether a specific, already-discovered piece of physical evidence
    // (i.e. currently visible on the shared CLUES/Evidence board — see
    // buildCluesBoard) genuinely came from the real Killer's accidental trace
    // (see resolve_kill) or was fabricated — either the Joker's deliberate
    // plant ('plant_joker_evidence') or the Accomplice's doctored frame job
    // ('accomplice_change_evidence'). Both fabrication paths are tagged
    // isPlanted: true at creation time (see findEvidenceEntryById); the
    // Killer's own genuine trace is explicitly tagged isPlanted: false.
    socket.on('verify_evidence', ({ code, evidenceId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game) {
            console.log('verify_evidence IGNORED: no active game for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('verify_evidence REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Forensic') {
            console.log(`verify_evidence REJECTED: ${socket.id} is role=${role || 'unknown'}, not Forensic`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): "verify evidence" is off the
        // table for the Forensic Examiner's entire penalty round, whichever
        // phase they're currently in.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'verify_evidence');
            socket.emit('verify_evidence_result', { code: targetRoom.code, success: false, reason: 'trap_debuff' });
            return;
        }

        if (!isForensicAbilityAvailable(game, socket.id, game.round)) {
            const lastUsed = game.forensicAbilityLastUsedRound?.[socket.id];
            const roundsRemaining = lastUsed == null ? 0 : (FORENSIC_SHARED_COOLDOWN_ROUNDS + 1) - (game.round - lastUsed);
            console.log(`verify_evidence REJECTED: ${socket.id} on shared Forensic cooldown`);
            socket.emit('verify_evidence_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                roundsRemaining: Math.max(0, roundsRemaining)
            });
            emitForensicAbilityStatus(targetRoom, socket.id);
            return;
        }

        const evidenceEntry = findEvidenceEntryById(targetRoom, evidenceId);
        if (!evidenceEntry || !evidenceEntry.foundBy || evidenceEntry.foundBy.length === 0) {
            console.log(`verify_evidence REJECTED: ${socket.id} chose evidence "${evidenceId}" not visible in room ${targetRoom.code}`);
            socket.emit('verify_evidence_result', { code: targetRoom.code, success: false, reason: 'invalid_evidence' });
            return;
        }

        const isAuthentic = !evidenceEntry.isPlanted && !evidenceEntry.isFalse;
        markForensicAbilityUsed(game, socket.id, game.round);
        emitForensicAbilityStatus(targetRoom, socket.id);

        console.log(`verify_evidence: room=${targetRoom.code} FORENSIC ${socket.id} checked "${evidenceEntry.text}" -> ${isAuthentic ? 'AUTHENTIC' : 'FABRICATED'}`);

        socket.emit('verify_evidence_result', {
            code: targetRoom.code,
            success: true,
            evidenceId: evidenceEntry.id,
            text: evidenceEntry.text,
            isAuthentic,
            roundsRemaining: FORENSIC_SHARED_COOLDOWN_ROUNDS + 1
        });
    });

    socket.on('get_forensic_report', ({ code, bodyId } = {}) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game) {
            console.log('get_forensic_report IGNORED: no active game for room', code);
            return;
        }

        const player = targetRoom.players.find(p => p.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('get_forensic_report REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Forensic') {
            console.log(`get_forensic_report REJECTED: ${socket.id} is role=${role || 'unknown'}, not Forensic`);
            return;
        }

        const game = targetRoom.game;
        if (!game.forensicReports) game.forensicReports = {};
        const reportsByBody = game.forensicReports[socket.id] && !Array.isArray(game.forensicReports[socket.id])
            ? game.forensicReports[socket.id]
            : {};
        const allReports = Object.values(reportsByBody);

        // The whole point of "View Report" is that it's scoped to the body the
        // Forensic is currently looking at — pull that specific entry out of
        // the per-body map instead of just handing back whichever examination
        // happened most recently across ALL bodies.
        const report = bodyId
            ? (reportsByBody[bodyId] || null)
            : (allReports[allReports.length - 1] || null);

        socket.emit('forensic_report', {
            code: targetRoom.code,
            success: Boolean(report),
            reason: report ? undefined : 'no_report',
            report,
            reports: allReports,
            examinedBodyIds: Object.keys(reportsByBody)
        });
    });

    socket.on('examine_body', ({ code, bodyId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'trial') {
            console.log('examine_body IGNORED: no active trial phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(p => p.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('examine_body REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Forensic') {
            console.log(`examine_body REJECTED: ${socket.id} is role=${role || 'unknown'}, not Forensic`);
            return;
        }

        const allBodies = [...(targetRoom.bodies || []), ...(targetRoom.discoveredBodies || [])];
        const body = allBodies.find(b => (b.playerId || b.nickname) === bodyId);
        if (!body || !body.foundBy || body.foundBy.length === 0) {
            console.log(`examine_body REJECTED: body "${bodyId}" not found or not yet discovered`);
            socket.emit('examine_body_result', { code: targetRoom.code, success: false, reason: 'invalid_body' });
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): no examining bodies during
        // the Forensic Examiner's penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'examine_body');
            socket.emit('examine_body_result', { code: targetRoom.code, success: false, reason: 'trap_debuff' });
            return;
        }

        if (!isForensicAbilityAvailable(game, socket.id, game.round)) {
            const lastUsed = game.forensicAbilityLastUsedRound?.[socket.id];
            const roundsRemaining = lastUsed == null ? 0 : (FORENSIC_SHARED_COOLDOWN_ROUNDS + 1) - (game.round - lastUsed);
            console.log(`examine_body REJECTED: ${socket.id} on shared Forensic cooldown`);
            socket.emit('examine_body_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                roundsRemaining: Math.max(1, roundsRemaining),
                available: false
            });
            emitForensicAbilityStatus(targetRoom, socket.id);
            return;
        }

        if (!game.forensicReports) game.forensicReports = {};
        // Migrate away from the old array-based history shape if a stale one
        // is somehow still present (e.g. an in-flight game from before this
        // fix), so a bad shape here never resurfaces the cross-body bug.
        const reportsByBody = game.forensicReports[socket.id] && !Array.isArray(game.forensicReports[socket.id])
            ? game.forensicReports[socket.id]
            : {};
        const existingReport = reportsByBody[bodyId] || null;

        if (existingReport) {
            const clue = { type: existingReport.type, value: existingReport.value };
            socket.emit('examine_body_result', {
                code: targetRoom.code,
                success: true,
                bodyId,
                clue,
                report: existingReport,
                reports: Object.values(reportsByBody),
                examinedBodyIds: Object.keys(reportsByBody)
            });
            return;
        }

        markForensicAbilityUsed(game, socket.id, game.round);
        const clue = resolveForensicClueForReveal(targetRoom, body);
        const report = {
            bodyId,
            bodyNickname: body.nickname,
            roomId: body.roomId,
            roomName: findMansionRoomById(body.roomId)?.name || body.roomId || 'Unknown room',
            round: body.round,
            type: clue?.type || null,
            value: clue?.value || null,
            savedAtRound: game.round
        };
        // Pinned to this specific bodyId — examining a different body later
        // can never clobber this entry.
        reportsByBody[bodyId] = report;
        game.forensicReports[socket.id] = reportsByBody;
        emitForensicAbilityStatus(targetRoom, socket.id);

        console.log(`examine_body: room=${targetRoom.code} FORENSIC ${socket.id} examined body "${bodyId}" -> ${clue ? clue.type : 'no clue'}`);

        socket.emit('examine_body_result', {
            code: targetRoom.code,
            success: true,
            bodyId,
            clue,
            report,
            reports: Object.values(reportsByBody),
            examinedBodyIds: Object.keys(reportsByBody)
        });
    });

    // The Joker's signature power: on their own turn, they may plant a piece of
    // physical evidence tied to their character into ANY mansion room of their
    // choosing (picked via the room-picker modal opened by the "PLANT
    // EVIDENCE" button under the map — see handlePlantJokerEvidence
    // client-side) — no longer tied to whatever room they happened to search
    // this turn. It's left behind for whoever searches that room later (see
    // plantedEvidenceForRoom / 'room_entered'), giving sharp players a way to
    // deduce the Joker's identity from their character's known hobbies. Gated
    // to once every JOKER_CLUE_COOLDOWN_TURNS of the Joker's OWN turns —
    // tracked server-side via game.jokerOwnTurnCount /
    // game.jokerClueLastUsedOwnTurn (see startPlayerTurn).
    socket.on('plant_joker_evidence', ({ code, roomId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('plant_joker_evidence IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('plant_joker_evidence REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const currentPlayerId = game.turnOrder[game.currentTurnIndex];
        if (currentPlayerId !== socket.id) {
            console.log(`plant_joker_evidence REJECTED: ${socket.id} is not the current player (expected ${currentPlayerId})`);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Joker') {
            console.log(`plant_joker_evidence REJECTED: ${socket.id} is role=${role || 'unknown'}, not Joker`);
            // Deliberately silent beyond this — a non-Joker shouldn't even have
            // this action available client-side, so no distinguishable reason is sent back.
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell — a Joker currently
        // confined there can't plant evidence anywhere else on their turn
        // either, same as every other action.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`plant_joker_evidence REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            socket.emit('joker_evidence_result', { code: targetRoom.code, success: false, reason: 'invalid_room' });
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): the Joker's "plant
        // evidence" ability is off the table for their entire penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'plant_joker_evidence');
            socket.emit('joker_evidence_result', { code: targetRoom.code, success: false, reason: 'trap_debuff' });
            return;
        }

        // Trust server-authoritative validation of the chosen room — it must be
        // a real, searchable mansion room. The holding cell is excluded: it's
        // spectator-only and no regular player ever searches it, so evidence
        // planted there could never be found.
        const roomInfo = findMansionRoomById(roomId);
        if (!roomInfo || roomId === 'f1_holding_cell') {
            console.log(`plant_joker_evidence REJECTED: ${socket.id} chose an invalid room "${roomId}"`);
            socket.emit('joker_evidence_result', { code: targetRoom.code, success: false, reason: 'invalid_room' });
            return;
        }

        if (!game.jokerOwnTurnCount) game.jokerOwnTurnCount = {};
        if (!game.jokerClueLastUsedOwnTurn) game.jokerClueLastUsedOwnTurn = {};
        const ownTurnCount = game.jokerOwnTurnCount[socket.id] || 1;
        const lastUsedOwnTurn = game.jokerClueLastUsedOwnTurn[socket.id];
        const turnsSinceUse = lastUsedOwnTurn == null ? null : ownTurnCount - lastUsedOwnTurn;
        if (lastUsedOwnTurn != null && turnsSinceUse < JOKER_CLUE_COOLDOWN_TURNS) {
            const turnsRemaining = JOKER_CLUE_COOLDOWN_TURNS - turnsSinceUse;
            console.log(`plant_joker_evidence REJECTED: ${socket.id} on cooldown, ${turnsRemaining} of their turn(s) remaining`);
            socket.emit('joker_evidence_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                turnsRemaining
            });
            return;
        }

        // Pick the next clue at RANDOM from this character's evidence pool,
        // never repeating one until every clue in the pool has been used once.
        // jokerCluePoolByPlayer holds a shuffled queue of remaining indices per
        // player, reshuffled from scratch whenever it runs dry — so the order
        // is random each cycle, but no clue repeats within a cycle.
        const evidencePool = CHARACTER_EVIDENCE[player.character] || [{ name: 'Mysterious personal item', description: 'An unidentified item with no further clues.' }];
        if (!game.jokerCluePoolByPlayer) game.jokerCluePoolByPlayer = {};
        let pool = game.jokerCluePoolByPlayer[socket.id];
        if (!pool || pool.length === 0) {
            pool = shuffleArray(evidencePool.map((_, i) => i));
        }
        const clueIndex = pool.shift();
        game.jokerCluePoolByPlayer[socket.id] = pool;
        const clueData = evidencePool[clueIndex];

        const clueEntry = {
            id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            text: clueData.name,
            description: clueData.description,
            roomId,
            roomName: roomInfo?.name || roomId,
            round: game.round,
            plantedAt: Date.now(),
            // Deliberately fabricated by the Joker, not left by the actual
            // Killer — the one case 'verify_evidence' (Forensic Examiner)
            // reports as FABRICATED/PLANTED.
            isPlanted: true
        };

        if (!targetRoom.plantedEvidence) targetRoom.plantedEvidence = {};
        if (!targetRoom.plantedEvidence[roomId]) targetRoom.plantedEvidence[roomId] = [];
        targetRoom.plantedEvidence[roomId].push(clueEntry);
        game.jokerClueLastUsedOwnTurn[socket.id] = ownTurnCount;

        console.log(`plant_joker_evidence: room=${targetRoom.code} JOKER ${socket.id} planted "${clueData.name}" in "${roomInfo?.name || roomId}"`);

        socket.emit('joker_evidence_result', {
            code: targetRoom.code,
            success: true,
            clue: clueEntry,
            turnsRemaining: JOKER_CLUE_COOLDOWN_TURNS
        });

        // Let any spectator currently peeking this room see the fresh evidence
        // immediately, without waiting for their next room switch.
        emitSpectatorRoomUpdates(targetRoom, roomId);
    });

    // Killer's Accomplice — "Set a Trap": mirrors the Joker's "PLANT EVIDENCE"
    // flow above almost exactly (same room-picker modal pattern, opened by
    // the "SET A TRAP" button under the map — see
    // handleChooseAccompliceTrapRoom client-side), but pins a trap to the
    // chosen mansion room instead of a piece of evidence. The trap is
    // recorded on the room (targetRoom.traps) and echoed back to the
    // Accomplice; whoever later walks into it (see triggerTrapIfPresent, via
    // either 'select_room' or 'use_vent') loses all actions and abilities for
    // their entire NEXT round (see isPlayerTrapDebuffed). Gated by its own
    // round-based cooldown (TRAP_COOLDOWN_ROUNDS), unlike
    // plant_joker_evidence's own-turn-based one.
    socket.on('set_trap', ({ code, roomId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('set_trap IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('set_trap REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const currentPlayerId = game.turnOrder[game.currentTurnIndex];
        if (currentPlayerId !== socket.id) {
            console.log(`set_trap REJECTED: ${socket.id} is not the current player (expected ${currentPlayerId})`);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Accomplice') {
            console.log(`set_trap REJECTED: ${socket.id} is role=${role || 'unknown'}, not Accomplice`);
            // Deliberately silent beyond this, same as plant_joker_evidence — a
            // non-Accomplice shouldn't even have this action available client-side.
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell — same as every other
        // action, including the Joker's plant evidence above.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`set_trap REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            socket.emit('set_trap_result', { code: targetRoom.code, success: false, reason: 'invalid_room' });
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): the Accomplice can't set a
        // NEW trap during their own penalty round either.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'set_trap');
            socket.emit('set_trap_result', { code: targetRoom.code, success: false, reason: 'trap_debuff' });
            return;
        }

        // Trust server-authoritative validation of the chosen room — same
        // rules as plant_joker_evidence: must be a real, searchable mansion
        // room, and never the (spectator-only) Holding Cell.
        const roomInfo = findMansionRoomById(roomId);
        if (!roomInfo || roomId === 'f1_holding_cell') {
            console.log(`set_trap REJECTED: ${socket.id} chose an invalid room "${roomId}"`);
            socket.emit('set_trap_result', { code: targetRoom.code, success: false, reason: 'invalid_room' });
            return;
        }

        if (!isTrapAbilityAvailable(game, socket.id, game.round)) {
            const payload = trapAbilityStatusPayload(targetRoom, socket.id);
            console.log(`set_trap REJECTED: ${socket.id} on cooldown, ${payload.roundsRemaining} round(s) remaining`);
            socket.emit('set_trap_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                roundsRemaining: payload.roundsRemaining
            });
            return;
        }

        const trapEntry = {
            id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            roomId,
            roomName: roomInfo?.name || roomId,
            round: game.round,
            setBy: socket.id,
            setAt: Date.now()
        };

        if (!targetRoom.traps) targetRoom.traps = {};
        if (!targetRoom.traps[roomId]) targetRoom.traps[roomId] = [];
        targetRoom.traps[roomId].push(trapEntry);
        markTrapAbilityUsed(game, socket.id, game.round);

        console.log(`set_trap: room=${targetRoom.code} ACCOMPLICE ${socket.id} set a trap in "${roomInfo?.name || roomId}"`);

        socket.emit('set_trap_result', {
            code: targetRoom.code,
            success: true,
            trap: trapEntry,
            roundsRemaining: TRAP_COOLDOWN_ROUNDS
        });

        // Let the Killer know where their Accomplice set a trap — same
        // "only the Killer/Accomplice pair learns this" privacy rule as the
        // accidental-evidence notice going the other way above.
        const killerId = findRoleHolderId(targetRoom, 'Killer');
        if (killerId && killerId !== socket.id) {
            io.to(killerId).emit('killer_trap_notice', {
                code: targetRoom.code,
                roomId,
                roomName: roomInfo?.name || roomId
            });
        }
    });

    // Killer's Accomplice — "Change Evidence" (active): a SEPARATE ability on
    // top of the normal "Investigate Room" action (which the Accomplice now
    // uses exactly like every other role, with no passive free reveal). Picks
    // one piece of REAL physical evidence already sitting in the room the
    // Accomplice is currently standing in and overwrites it in place with
    // fabricated evidence that points at a chosen (non-self) player — same
    // "server never says who actually did it" anonymity every other
    // planted-evidence path follows: nothing here ever reveals the
    // Accomplice was the one who altered it. Not tied to the once-per-turn
    // room-interaction slot (see roomActionUsedThisTurn) at all; it's gated
    // purely by its own cooldown, once every ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS
    // of the Accomplice's OWN turns (see startPlayerTurn /
    // game.accompliceEvidenceLastUsedOwnTurn).
    socket.on('accomplice_change_evidence', ({ code, roomId, evidenceId, targetPlayerId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('accomplice_change_evidence IGNORED: no active action phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(pl => pl.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('accomplice_change_evidence REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Accomplice') {
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} is role=${role || 'unknown'}, not Accomplice`);
            // Deliberately silent beyond this — a non-Accomplice shouldn't even
            // have this action available client-side, so no distinguishable
            // reason is sent back.
            return;
        }

        // Trust server-authoritative state for where the player actually is,
        // never the roomId the client happens to send — same rule
        // 'investigate_room' / 'search_body' apply.
        const actualRoomId = game.playerLocations?.[socket.id];
        if (!actualRoomId || actualRoomId !== roomId) {
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} is not currently standing in room ${roomId}`);
            return;
        }

        // Room Restrictions: NO Actions in Holding Cell.
        if (isConfinedToHoldingCell(game, socket.id)) {
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} is confined to the Holding Cell this round`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): "change evidence" is off
        // the table for the Accomplice's entire penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'accomplice_change_evidence');
            return;
        }

        if (game.pendingKillDecision && game.pendingKillDecision.killerId === socket.id) {
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} has a pending post-kill decision`);
            return;
        }

        // The target evidence must be a REAL entry currently sitting in the
        // room the Accomplice is standing in right now — never trust an id
        // for evidence in some other room, or one that's already been
        // overwritten by an earlier use of this same ability.
        const roomEvidence = targetRoom.plantedEvidence?.[actualRoomId] || [];
        const evidenceEntry = roomEvidence.find(entry => entry.id === evidenceId);
        if (!evidenceEntry) {
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} chose evidence "${evidenceId}" not present in room ${actualRoomId}`);
            socket.emit('accomplice_evidence_result', { code: targetRoom.code, success: false, reason: 'invalid_evidence' });
            return;
        }

        // Target must be a real, currently active player — never eliminated,
        // never an observer/spectator, and strictly never the Accomplice
        // themselves.
        const targetPlayer = targetRoom.players.find(p => p.id === targetPlayerId);
        if (!targetPlayer || targetPlayer.isEliminated || targetPlayer.isObserver) {
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} chose an invalid/inactive target ${targetPlayerId}`);
            socket.emit('accomplice_evidence_result', { code: targetRoom.code, success: false, reason: 'invalid_target' });
            return;
        }
        if (targetPlayer.id === socket.id) {
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} tried to frame themselves`);
            socket.emit('accomplice_evidence_result', { code: targetRoom.code, success: false, reason: 'invalid_target' });
            return;
        }

        if (!game.accompliceOwnTurnCount) game.accompliceOwnTurnCount = {};
        if (!game.accompliceEvidenceLastUsedOwnTurn) game.accompliceEvidenceLastUsedOwnTurn = {};
        const ownTurnCount = game.accompliceOwnTurnCount[socket.id] || 1;
        const lastUsedOwnTurn = game.accompliceEvidenceLastUsedOwnTurn[socket.id];
        const turnsSinceUse = lastUsedOwnTurn == null ? null : ownTurnCount - lastUsedOwnTurn;
        if (lastUsedOwnTurn != null && turnsSinceUse < ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS) {
            const turnsRemaining = ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS - turnsSinceUse;
            console.log(`accomplice_change_evidence REJECTED: ${socket.id} on cooldown, ${turnsRemaining} of their turn(s) remaining`);
            socket.emit('accomplice_evidence_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                turnsRemaining
            });
            return;
        }

        // --- STATE UPDATE: overwrite the real evidence in place with false
        // evidence pointing at the chosen target. The entry's id/roomId/round
        // stay put (so any existing 'foundBy' discoverers keep their credit
        // and the CLUES board entry doesn't fork into a duplicate) — only the
        // content and framing metadata change.
        const framed = buildFramedEvidenceEntry(game, targetPlayer);
        evidenceEntry.text = framed.text;
        evidenceEntry.description = framed.description;
        evidenceEntry.isFalse = true;
        // Same flag 'verify_evidence' (Forensic Examiner) checks for every
        // other fabrication source (see plant_joker_evidence) — doctoring
        // real evidence to frame someone else is just as inauthentic as
        // planting it from scratch.
        evidenceEntry.isPlanted = true;
        evidenceEntry.framedPlayerId = targetPlayer.id;
        evidenceEntry.framedNickname = targetPlayer.nickname;

        game.accompliceEvidenceLastUsedOwnTurn[socket.id] = ownTurnCount;

        console.log(`accomplice_change_evidence: room=${targetRoom.code} ACCOMPLICE ${socket.id} altered evidence "${evidenceId}" in "${actualRoomId}" to frame ${targetPlayer.nickname}`);

        socket.emit('accomplice_evidence_result', {
            code: targetRoom.code,
            success: true,
            evidence: { id: evidenceEntry.id, text: evidenceEntry.text },
            turnsRemaining: ACCOMPLICE_EVIDENCE_COOLDOWN_TURNS
        });

        // If this evidence was already on the shared CLUES board (i.e. someone
        // had genuinely investigated it before it got doctored), push a fresh
        // board out to everyone — buildCluesBoard now excludes anything with
        // framedPlayerId set, so this makes the entry quietly vanish from
        // everyone's board instead of leaving the old real text sitting there.
        // The frame job itself is never disclosed to the group; it only stays
        // known to whoever personally walks in and investigates this room
        // from here on.
        if (evidenceEntry.foundBy && evidenceEntry.foundBy.length > 0) {
            io.to(targetRoom.id).emit('clues_board_update', { code: targetRoom.code, clues: buildCluesBoard(targetRoom) });
        }

        // Let any spectator currently peeking this room see the altered
        // evidence immediately, without waiting for their next room switch.
        emitSpectatorRoomUpdates(targetRoom, actualRoomId);
    });


    socket.on('end_turn', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'action') {
            console.log('end_turn IGNORED: no active action phase for room', code);
            return;
        }

        const currentPlayerId = targetRoom.game.turnOrder[targetRoom.game.currentTurnIndex];
        if (currentPlayerId !== socket.id) {
            console.log(`end_turn REJECTED: ${socket.id} is not the current player (expected ${currentPlayerId})`);
            return;
        }

        console.log(`end_turn: player ${socket.id} completed their turn in room ${targetRoom.code}`);
        clearTurnTimer(targetRoom.id);
        advanceTurn(targetRoom);
    });

    // Innocent-only "win button": submitting the fully-assembled override code
    // during the trial phase ends the match immediately. The server is the sole
    // authority on both WHO may attempt this (role must be Innocent, checked
    // server-side regardless of what the client shows) and WHAT counts as
    // correct (compared against the match's own digitalCode, never anything
    // the client asserts).
    socket.on('submit_innocent_code', ({ code, guess }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'trial') {
            console.log('submit_innocent_code IGNORED: no active trial phase for room', code);
            return;
        }

        const player = targetRoom.players.find(p => p.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('submit_innocent_code REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Innocent') {
            console.log(`submit_innocent_code REJECTED: ${socket.id} is role=${role || 'unknown'}, not Innocent`);
            // Deliberately silent to the client beyond this — a non-Innocent
            // shouldn't even have this terminal available, so no need to hand
            // back a distinguishable rejection reason.
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): the exit terminal refuses
        // this Innocent's input entirely during their penalty round.
        if (isPlayerTrapDebuffed(targetRoom.game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'submit_innocent_code');
            socket.emit('code_submission_result', {
                code: targetRoom.code,
                success: false,
                reason: 'trap_debuff',
                message: "You're still rattled from the trap — the terminal won't accept input from you this round."
            });
            return;
        }

        // The Innocents physically cannot leave while a body is still
        // unaccounted for — if the Killer has struck and nobody has found
        // that victim yet (see creditExposedBodyDiscovery / 'search_body'),
        // the exit terminal refuses input outright, correct code or not.
        if (hasUndiscoveredBody(targetRoom)) {
            console.log(`submit_innocent_code REJECTED: room=${targetRoom.code} an undiscovered body is still out there`);
            socket.emit('code_submission_result', {
                code: targetRoom.code,
                success: false,
                reason: 'body_missing',
                message: 'The exit is sealed — a body is still out there, unaccounted for.'
            });
            return;
        }

        const submitted = String(guess ?? '').trim();
        const isCorrect = submitted.length > 0 && submitted === targetRoom.digitalCode;

        if (isCorrect) {
            console.log(`submit_innocent_code: room=${targetRoom.code} CORRECT code entered by ${player.nickname} (${socket.id})`);
            endGameWithVictory(targetRoom, 'Innocent', {
                reason: 'CODE_CRACKED',
                message: `${player.nickname} cracked the override code. The Innocents win!`,
                triggeredBy: { id: player.id, nickname: player.nickname }
            });
        } else {
            console.log(`submit_innocent_code: room=${targetRoom.code} INCORRECT guess from ${socket.id}`);
            socket.emit('code_submission_result', {
                code: targetRoom.code,
                success: false,
                message: 'Invalid Code'
            });
        }
    });

    // A ballot is stored only after its owner explicitly locks it. Public voting
    // updates intentionally contain confirmation IDs, never targets or tallies.
    socket.on('confirm_vote', ({ code, targetId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'trial') {
            console.log('confirm_vote IGNORED: no active trial phase for room', code);
            return;
        }

        const player = targetRoom.players.find(p => p.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('confirm_vote REJECTED: player is eliminated, observing, or not found', socket.id);
            return;
        }

        const trial = targetRoom.game.trial;
        if (!trial || trial.status !== 'voting') {
            console.log('confirm_vote REJECTED: trial not accepting votes', targetRoom.code);
            return;
        }

        if (trial.eligibleVoterIds && !trial.eligibleVoterIds.includes(socket.id)) {
            console.log('confirm_vote REJECTED: player not eligible', socket.id);
            return;
        }

        // A vote must target another active (votable) candidate.
        if (targetId && trial.eligibleVoterIds && !trial.eligibleVoterIds.includes(targetId)) {
            console.log('confirm_vote REJECTED: target is not a votable candidate', targetId);
            return;
        }
        if (targetId === socket.id) {
            const voterRole = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
            // The Joker is exempt from the usual no-self-vote rule — everyone
            // else still cannot vote for themselves.
            if (voterRole !== 'Joker') {
                console.log('confirm_vote REJECTED: players cannot vote for themselves', socket.id);
                return;
            }
        }

        trial.votes[socket.id] = targetId || null;
        if (!trial.confirmedVoterIds) trial.confirmedVoterIds = [];
        if (!trial.confirmedVoterIds.includes(socket.id)) trial.confirmedVoterIds.push(socket.id);

        io.to(targetRoom.id).emit('trial_vote_update', {
            code: targetRoom.code,
            confirmedVoterIds: trial.confirmedVoterIds,
            totalEligible: trial.eligibleVoterIds.length
        });

        console.log(`confirm_vote: room=${targetRoom.code} voter=${socket.id} locked`);

        if (trial.eligibleVoterIds.every(id => trial.confirmedVoterIds.includes(id))) {
            resolveTrialPhase(targetRoom);
        }
    });

    socket.on('unlock_vote', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        const trial = targetRoom?.game?.trial;
        if (!targetRoom || targetRoom.game.phase !== 'trial' || !trial || trial.status !== 'voting') return;
        if (!trial.eligibleVoterIds?.includes(socket.id)) return;
        if (trial.eligibleVoterIds.every(id => (trial.confirmedVoterIds || []).includes(id))) return;

        trial.confirmedVoterIds = (trial.confirmedVoterIds || []).filter(id => id !== socket.id);
        delete trial.votes[socket.id];
        io.to(targetRoom.id).emit('trial_vote_update', {
            code: targetRoom.code,
            confirmedVoterIds: trial.confirmedVoterIds,
            totalEligible: trial.eligibleVoterIds.length
        });
        console.log(`unlock_vote: room=${targetRoom.code} voter=${socket.id} unlocked`);
    });

    // Detective-only Court/Trial phase ability: reveal exactly which room a
    // chosen player ended their previous turn in. Gated on a 2-round cooldown
    // (DETECTIVE_ABILITY_COOLDOWN_ROUNDS) tracked in
    // game.detectiveAbilityLastUsedRound. The answer itself comes from
    // game.lastKnownEndRoom, snapshotted once per round the instant the
    // Court/Trial phase begins (see startTrialPhase) — and the result is
    // ALWAYS emitted only to the requesting socket, never broadcast, so no
    // other player (including the target) ever learns this happened.
    socket.on('detective_check_location', ({ code, targetId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'trial') {
            console.log('detective_check_location IGNORED: no active trial phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(p => p.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('detective_check_location REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Detective') {
            console.log(`detective_check_location REJECTED: ${socket.id} is role=${role || 'unknown'}, not Detective`);
            // Deliberately silent beyond this — a non-Detective shouldn't even
            // have this action available client-side, so no distinguishable
            // reason is sent back.
            return;
        }

        const targetPlayer = targetRoom.players.find(p => p.id === targetId);
        if (!targetPlayer) {
            console.log('detective_check_location REJECTED: unknown target', targetId);
            return;
        }

        // The Detective already knows their own location — targeting yourself
        // isn't a real investigation, so it's rejected the same silent way an
        // invalid role check is above (a correctly-behaving client shouldn't
        // even offer itself as a selectable target).
        if (targetId === socket.id) {
            console.log(`detective_check_location REJECTED: ${socket.id} tried to target themselves`);
            return;
        }

        // Trap debuff (see isPlayerTrapDebuffed): no location checks during
        // the Detective's penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'detective_check_location');
            socket.emit('detective_check_result', { code: targetRoom.code, success: false, reason: 'trap_debuff' });
            return;
        }

        if (!game.detectiveAbilityLastUsedRound) game.detectiveAbilityLastUsedRound = {};
        const lastUsedRound = game.detectiveAbilityLastUsedRound[socket.id];
        const roundsSinceUse = lastUsedRound == null ? null : game.round - lastUsedRound;
        if (lastUsedRound != null && roundsSinceUse < DETECTIVE_ABILITY_COOLDOWN_ROUNDS) {
            const turnsRemaining = DETECTIVE_ABILITY_COOLDOWN_ROUNDS - roundsSinceUse;
            console.log(`detective_check_location REJECTED: ${socket.id} on cooldown, ${turnsRemaining} round(s) remaining`);
            socket.emit('detective_check_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                turnsRemaining
            });
            return;
        }

        const roomId = (game.lastKnownEndRoom || game.playerLocations || {})[targetId] || null;
        const roomInfo = roomId ? findMansionRoomById(roomId) : null;

        game.detectiveAbilityLastUsedRound[socket.id] = game.round;

        console.log(`detective_check_location: room=${targetRoom.code} DETECTIVE ${socket.id} checked ${targetId} -> ${roomInfo?.name || 'unknown (no recorded location)'}`);

        // Strictly private: only the Detective's own socket ever receives this.
        socket.emit('detective_check_result', {
            code: targetRoom.code,
            success: true,
            targetId,
            targetNickname: targetPlayer.nickname,
            roomId: roomId || null,
            roomName: roomInfo?.name || 'Unknown Location',
            turnsRemaining: DETECTIVE_ABILITY_COOLDOWN_ROUNDS
        });
    });

    // Officer-only Court/Trial phase ability: lock ONE player (including the
    // Officer themself) into the Holding Cell for the ENTIRE NEXT round.
    // Gated on a 3-round cooldown (OFFICER_ABILITY_COOLDOWN_ROUNDS) tracked in
    // game.officerAbilityLastUsedRound. This does not move anyone immediately
    // — it schedules the lock via game.holdingCellLock, which startNewRound
    // actually applies once the next round begins (see there). Like the
    // Detective's ability, USING it is private to the Officer; unlike the
    // Detective's ability, the EFFECT becomes public once the locked round
    // starts (see the 'round_start' broadcast in startNewRound) — every
    // player can plainly see that seat empty for the whole round.
    socket.on('officer_lock_player', ({ code, targetId }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom || !targetRoom.game || targetRoom.game.phase !== 'trial') {
            console.log('officer_lock_player IGNORED: no active trial phase for room', code);
            return;
        }

        const game = targetRoom.game;
        const player = targetRoom.players.find(p => p.id === socket.id);
        if (!player || player.isEliminated || player.isObserver) {
            console.log('officer_lock_player REJECTED: player eliminated, observing, or not found', socket.id);
            return;
        }

        const role = targetRoom.roles ? targetRoom.roles[socket.id] : undefined;
        if (role !== 'Officer') {
            console.log(`officer_lock_player REJECTED: ${socket.id} is role=${role || 'unknown'}, not Officer`);
            // Deliberately silent beyond this — a non-Officer shouldn't even
            // have this action available client-side, so no distinguishable
            // reason is sent back.
            return;
        }

        const targetPlayer = targetRoom.players.find(p => p.id === targetId);
        if (!targetPlayer || targetPlayer.isEliminated || targetPlayer.isObserver) {
            console.log('officer_lock_player REJECTED: unknown or inactive target', targetId);
            return;
        }

        // Unlike the Detective's ability, the Officer MAY target themselves —
        // deliberately no rejection here for targetId === socket.id.

        // Trap debuff (see isPlayerTrapDebuffed): no locking anyone up during
        // the Officer's own penalty round.
        if (isPlayerTrapDebuffed(game, socket.id)) {
            rejectForTrapDebuff(targetRoom, socket, 'officer_lock_player');
            socket.emit('officer_lock_result', { code: targetRoom.code, success: false, reason: 'trap_debuff' });
            return;
        }

        if (!game.officerAbilityLastUsedRound) game.officerAbilityLastUsedRound = {};
        const lastUsedRound = game.officerAbilityLastUsedRound[socket.id];
        const roundsSinceUse = lastUsedRound == null ? null : game.round - lastUsedRound;
        if (lastUsedRound != null && roundsSinceUse < OFFICER_ABILITY_COOLDOWN_ROUNDS) {
            const turnsRemaining = OFFICER_ABILITY_COOLDOWN_ROUNDS - roundsSinceUse;
            console.log(`officer_lock_player REJECTED: ${socket.id} on cooldown, ${turnsRemaining} round(s) remaining`);
            socket.emit('officer_lock_result', {
                code: targetRoom.code,
                success: false,
                reason: 'cooldown',
                turnsRemaining
            });
            return;
        }

        game.officerAbilityLastUsedRound[socket.id] = game.round;
        game.holdingCellLock = {
            targetId,
            targetNickname: targetPlayer.nickname,
            lockedForRound: game.round + 1
        };

        console.log(`officer_lock_player: room=${targetRoom.code} OFFICER ${socket.id} scheduled ${targetId} (${targetPlayer.nickname}) to be locked in the Holding Cell for round ${game.round + 1}`);

        // Strictly private: only the Officer's own socket ever receives
        // confirmation that the lock was successfully scheduled.
        socket.emit('officer_lock_result', {
            code: targetRoom.code,
            success: true,
            targetId,
            targetNickname: targetPlayer.nickname,
            turnsRemaining: OFFICER_ABILITY_COOLDOWN_ROUNDS
        });
    });

    socket.on('send_chat_message', ({ code, message }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (!targetRoom) return;

        const sender = targetRoom.players.find(p => p.id === socket.id);
        if (!sender) return;
        if (targetRoom.game?.phase === 'trial' && (sender.isEliminated || sender.isObserver)) {
            console.log(`send_chat_message REJECTED: spectator ${socket.id} cannot discuss during trial`);
            return;
        }

        const text = (message || '').trim();
        if (!text) return;

        const payload = {
            id: `${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
            senderId: socket.id,
            senderNickname: sender?.nickname || 'Agent',
            text,
            timestamp: Date.now()
        };

        if (!targetRoom.chatMessages) targetRoom.chatMessages = [];
        targetRoom.chatMessages.push(payload);
        targetRoom.chatMessages = targetRoom.chatMessages.slice(-80);

        io.to(targetRoom.id).emit('chat_message', { code: targetRoom.code, message: payload });
    });

    socket.on('leave_room', ({ code }) => {
        const targetRoom = Object.values(rooms).find(r => r.code === code);
        if (targetRoom) {
            targetRoom.players = targetRoom.players.filter(p => p.id !== socket.id);
            socket.leave(targetRoom.id);

            handlePlayerLeftRoom(targetRoom, socket.id);

            if (targetRoom.players.length === 0) {
                clearTurnTimer(targetRoom.id);
                clearTrialTickTimer(targetRoom.id);
                clearTrialTransitionTimers(targetRoom.id);
                clearGameOverTimer(targetRoom.id);
                delete rooms[targetRoom.id];
                console.log(`Room ${targetRoom.code} deleted (empty)`);
            } else {
                logRoomState('leave_room', targetRoom);
                io.to(targetRoom.id).emit('room_updated', roomUpdatedPayload(targetRoom));
            }

            io.emit('rooms_list', publicRoomsList());
        }
    });

    socket.on('disconnect', () => {
        console.log(`Player disconnected: ${socket.id}`);

        Object.keys(rooms).forEach(roomId => {
            const room = rooms[roomId];
            const initialLength = room.players.length;
            room.players = room.players.filter(p => p.id !== socket.id);

            if (room.players.length !== initialLength) {
                handlePlayerLeftRoom(room, socket.id);

                if (room.players.length === 0) {
                    clearTurnTimer(room.id);
                    clearTrialTickTimer(room.id);
                    clearTrialTransitionTimers(room.id);
                    clearGameOverTimer(room.id);
                    delete rooms[roomId];
                    console.log(`Room ${room.code} deleted (empty)`);
                } else {
                    logRoomState('disconnect cleanup', room);
                    io.to(roomId).emit('room_updated', roomUpdatedPayload(room));
                }
            }
        });

        io.emit('rooms_list', publicRoomsList());
    });
});

const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
    console.log(`Server successfully running on port ${PORT}`);
});