# Shell Completions

> **Status:** COMPLETED (2026-01-26)

Auto-generate bash/zsh/fish completions for the `buquet` CLI.

## Why

First-run experience is critical. Tab completion makes the tool feel polished and discoverable. Users can explore commands without reading docs.

## Usage

```bash
# Bash
buquet completions bash > ~/.local/share/bash-completion/completions/buquet

# Zsh (add to fpath)
buquet completions zsh > ~/.zfunc/_buquet

# Fish
buquet completions fish > ~/.config/fish/completions/buquet.fish

# PowerShell
buquet completions powershell > buquet.ps1

# Elvish
buquet completions elvish > buquet.elv
```

After installing, restart your shell or source the completion file.

## Implementation

Added `clap_complete` dependency and a `Completions` subcommand:

```rust
// crates/buquet/src/cli/commands.rs
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
// crates/buquet/src/main.rs
use clap::CommandFactory;
use clap_complete::generate;

// Handle before S3 init (doesn't need credentials)
if let Commands::Completions { shell } = cli.command {
    let mut cmd = Cli::command();
    generate(shell, &mut cmd, "buquet", &mut io::stdout());
    return Ok(());
}
```

## Files Changed

- `Cargo.toml` - Added `clap_complete = "4"`
- `crates/buquet/src/cli/commands.rs` - Added `Completions` variant
- `crates/buquet/src/main.rs` - Added handler before S3 initialization
- `docs/getting-started.md` - Added usage documentation

## Effort

~15 lines of Rust

## Dependencies

`clap_complete` (companion crate to clap)
