# CLAUDE.md - Site Development Guide

## Build Commands
- `hugo server -D` - Start local development server with drafts enabled
- `hugo` - Build site without drafts
- `hugo --minify` - Build minified site (used in production)
- `hugo new content/ko/posts/yyyy/title.md` - Create new post

## Deployment
- Automatic deployment via GitHub Actions on push to `src` branch
- Hugo (extended version) compiles site which is deployed to `main` branch

## Style Guidelines
- **Content**: Korean language content in `/content/ko/`
- **Frontmatter**: Include title, date, draft status (see template in existing posts)
- **Post URLs**: Follow the pattern `/year/month/day/slug/`
- **Code Blocks**: Use triple backticks with language identifier
- **Images**: Store in static directory with relative links

## Theme
- Using PaperMod theme as Git submodule
- Configuration in `config.toml`
- Syntax highlighting via Goldmark renderer (Hugo default)