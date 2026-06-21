#!/bin/bash

# Configuration
BACKUP_DIR="/root/indigo/backup"

# Restore Hermes Agent state.db
cp $BACKUP_DIR/hermes_state_*.db <hermes-root>/state.db

# Restore Hermes Agent state-snapshots
cp -r $BACKUP_DIR/hermes_state_snapshots_* <hermes-root>/state-snapshots/

# Restore Chronicle (Elephas)
cp $BACKUP_DIR/chronicle_lbug_* <hermes-root>/commons/db/ocas-elephas/chronicle.lbug
cp $BACKUP_DIR/chronicle_lbug_backup_* <hermes-root>/prep_preservation/chronicle.lbug

# Restore Weave (social graph)
cp $BACKUP_DIR/weave_lbug_* /root/indigo-repo/commons/db/ocas-weave/weave.lbug
cp $BACKUP_DIR/weave_lbug_*_hermes <hermes-root>/commons/db/ocas-weave/weave.lbug
cp $BACKUP_DIR/weave_lbug_backup_* <hermes-root>/prep_preservation/weave.lbug
cp $BACKUP_DIR/weave_lbug_data_* <hermes-root>/data/hermes-weave/weave.lbug

# Restore Styx (transaction data)
cp $BACKUP_DIR/styx_db_* <hermes-root>/data/styx.db
cp $BACKUP_DIR/transactions_db_* <hermes-root>/data/transactions.db

# Restore MemPalace
tar -xzf $BACKUP_DIR/mempalace_*.tar.gz -C /root/.mempalace/
cp $BACKUP_DIR/chroma_db_* /root/.mempalace/palace/chroma.sqlite3

# Restore operating files
# Add any additional files you need to restore here

# Restart Hermes Agent if needed
systemctl restart hermes-agent

