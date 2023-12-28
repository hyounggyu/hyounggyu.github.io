# hyounggyu.github.io

## Issues

### Syntax Highlighting Using Chroma

Ref: https://adityatelange.github.io/hugo-PaperMod/posts/papermod/papermod-faq/

다음과 같이 `config.toml` 파일을 수정하라고 하는데, 적용이 안된다.

```toml
[params.assets]
disableHLJS = true

[markup.highlight]
codeFences = true
guessSyntax = true
lineNos = true
style = "monokai"
```

결국 포기하고 그냥 기본 설정 사용
