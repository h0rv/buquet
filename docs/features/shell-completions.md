# Shell Completions

> **Status:** COMPLETED (2026-01-26)

Auto-generate bash/zsh/fish completions for the `oq` CLI.

## Why

First-run experience is critical. Tab completion makes the tool feel polished and discoverable. Users can explore commands without reading docs.

## Usage

```bash
# Bash
oq completions bash > ~/.local/share/bash-completion/completions/oq

# Zsh (add to fpath)
oq completions zsh > ~/.zfunc/_oq

# Fish
oq completions fish > ~/.config/fish/completions/oq.fish

# PowerShell
oq completions powershell > oq.ps1

# Elvish
oq completions elvish > oq.elv
```

After installing, restart your shell or source the completion file.

## Implementation

Added `clap_complete` dependency and a `Completions` subcommand:

```rust
// crates/oq/src/cli/commands.rs
use clap_complete::Shell;

#[derive(Subcommand, Debug)]
pub enum Commands {
    // ... existing commands

    /// Generate shell completions
    Completions {
        #[arg(value_enum)]
        shell: Shell,
    },
}
```

```rust
// crates/oq/src/main.rs
use clap::CommandFactory;
use clap_complete::generate;

// Handle before S3 init (doesn't need credentials)
if let Commands::Completions { shell } = cli.command {
    let mut cmd = Cli::command();
    generate(shell, &mut cmd, "oq", &mut io::stdout());
    return Ok(());
}
```

## Files Changed

- `Cargo.toml` - Added `clap_complete = "4"`
- `crates/oq/src/cli/commands.rs` - Added `Completions` variant
- `crates/oq/src/main.rs` - Added handler before S3 initialization
- `docs/getting-started.md` - Added usage documentation

## Effort

~15 lines of Rust

## Dependencies

`clap_complete` (companion crate to clap)
