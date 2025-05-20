import typer
from typing import Optional

# Create Typer app instances
app = typer.Typer()
image_app = typer.Typer()
sandbox_app = typer.Typer()
process_app = typer.Typer()

# Register subapps
app.add_typer(image_app, name="image")
app.add_typer(sandbox_app, name="sandbox")
app.add_typer(process_app, name="process")

# Image commands
@image_app.command("create")
def image_create(
    name: str = typer.Argument(..., help="Name of the image"),
    provider: str = typer.Option("modal", help="Provider to use (e.g., modal, docker)"),
    tag: Optional[str] = typer.Option(None, help="Tag for the image"),
):
    """Create a new image with the specified provider."""
    typer.echo(f"Creating image '{name}' with provider '{provider}'{f' and tag {tag}' if tag else ''}...")

@image_app.command("list")
def image_list(
    all: bool = typer.Option(False, "--all", "-a", help="Show all images, including inactive ones"),
):
    """List all available images."""
    typer.echo(f"Listing images (all={all})...")

@image_app.command("destroy")
def image_destroy(
    name: str = typer.Argument(..., help="Name of the image to destroy"),
    force: bool = typer.Option(False, "--force", "-f", help="Force destroy without confirmation"),
):
    """Destroy an existing image."""
    if not force:
        confirm = typer.confirm(f"Are you sure you want to destroy image '{name}'?")
        if not confirm:
            typer.echo("Operation cancelled.")
            return
    
    typer.echo(f"Destroying image '{name}'...")

# Sandbox commands
@sandbox_app.command("create")
def sandbox_create(
    name: str = typer.Argument(..., help="Name of the sandbox"),
    image: str = typer.Option(..., help="Image to use"),
    provider: str = typer.Option("modal", help="Provider to use"),
):
    """Create a new sandbox using the specified image."""
    typer.echo(f"Creating sandbox '{name}' using image '{image}' with provider '{provider}'...")

@sandbox_app.command("list")
def sandbox_list(
    all: bool = typer.Option(False, "--all", "-a", help="Show all sandboxes including stopped ones"),
):
    """List all sandboxes."""
    typer.echo(f"Listing sandboxes (all={all})...")

@sandbox_app.command("destroy")
def sandbox_destroy(
    name: str = typer.Argument(..., help="Name of the sandbox to destroy"),
    force: bool = typer.Option(False, "--force", "-f", help="Force destroy without confirmation"),
):
    """Destroy an existing sandbox."""
    if not force:
        confirm = typer.confirm(f"Are you sure you want to destroy sandbox '{name}'?")
        if not confirm:
            typer.echo("Operation cancelled.")
            return
    
    typer.echo(f"Destroying sandbox '{name}'...")

# Process commands
@process_app.command("create")
def process_create(
    name: str = typer.Argument(..., help="Name of the process"),
    sandbox: str = typer.Option(..., help="Sandbox to run in"),
    command: str = typer.Option(..., help="Command to execute"),
):
    """Create a new process in the specified sandbox."""
    typer.echo(f"Creating process '{name}' in sandbox '{sandbox}' with command '{command}'...")

@process_app.command("list")
def process_list(
    all: bool = typer.Option(False, "--all", "-a", help="Show all processes including completed ones"),
):
    """List all processes."""
    typer.echo(f"Listing processes (all={all})...")

@process_app.command("destroy")
def process_destroy(
    name: str = typer.Argument(..., help="Name of the process to destroy"),
    force: bool = typer.Option(False, "--force", "-f", help="Force destroy without confirmation"),
):
    """Destroy an existing process."""
    if not force:
        confirm = typer.confirm(f"Are you sure you want to destroy process '{name}'?")
        if not confirm:
            typer.echo("Operation cancelled.")
            return
    
    typer.echo(f"Destroying process '{name}'...")

if __name__ == "__main__":
    app()