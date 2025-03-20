# hyounggyu.github.io

Personal blog built with Hugo and PaperMod theme.

## Local Development

```bash
# Install Hugo (Extended version required)
brew install hugo

# Start development server
hugo server -D

# Build site for production
hugo --minify
```

## Features

- Fast and responsive theme (PaperMod)
- Syntax highlighting with Chroma
- Multilingual support (Korean)
- Search functionality
- Dark/light mode

## Configuration

The site is configured through `config.toml`. See [PaperMod documentation](https://github.com/adityatelange/hugo-PaperMod/wiki) for theme options.

## Content Management

Create new posts with:

```bash
hugo new content/ko/posts/yyyy/title.md
```

## Deployment

The site is automatically deployed via GitHub Actions when changes are pushed to the `src` branch.
