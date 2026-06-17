---
title: "SW V&V 문서 작성에 AI 에이전트 활용하기"
date: 2026-06-17T23:30:00+09:00
draft: false
slug: "sw-vv-writing-with-ai-agent"
---

<iframe width="560" height="315" src="https://www.youtube.com/embed/IVviuzu2ypM?si=Rzkc-XSyyZxAQfFg" title="YouTube video player" frameborder="0" allow="accelerometer; autoplay; clipboard-write; encrypted-media; gyroscope; picture-in-picture; web-share" referrerpolicy="strict-origin-when-cross-origin" allowfullscreen></iframe>

예제 프로젝트: https://github.com/hyounggyu/swvnv

항상 코드를 작성하는데 급급해서 문서까지 손을 대지 못했다. 소프트웨어는 소스 코드만으로 이루어지지 않는다는 것을 알면서도 그렇다. 코드를 주면 글로 만들어주는 마법이 있으면 얼마나 좋을까? 이제 그 기대가 현실이 되었다. 글을 쓰면 코드를 주고, 코드를 주면 글을 써준다.

헬스케어분야의 소프트웨어 개발에는 소프트웨어 검증 및 유효성 확인(software verification and validation) 문서가 필수다. 이 문서는 설계부터 테스트까지 써야하는데 추적성을 위해서 각 항목들을 모두 연결지어서 관리해야한다. 그래서 항목 하나 변경하면 여러 문서를 동시에 수정해야한다.

이 일에 AI 에이전트를 활용하기는 해야겠는데 MS Word로 작업하는 것은 상상이 안되었다(이 마법의 세계에서는 상상하지 못하면 구현할 수 없다). 그래서 택한 것이 Typst다. 처음엔 TeX이 워낙 잘 만든 도구이면서 오랜 시간 검증된 도구라 Typst를 굳이 도입해야할까라는 의구심이 들었다. 하지만 이내 아무렴 어떠하랴 맘에 안들면 TeX으로 전환하지 뭐. 라고 생각하고 시작했다.

Typst는 쓰면 쓸수록 정말 잘 만든 조판 도구라는 생각이다. 특히, AI 에이전트에 잘 맞는다는 측면에서 그렇다. 예전 같으면 소규모 커뮤니티에서 몇 없는 자료를 읽으며 힘들게 조판했겠지만 이제는 에이전트가 대신 코딩해주니까 어려울 게 없었다.

초반에 Typst로 문서 템플릿을 만들고, 그 다음 내용을 채우기 시작했다. "코드를 주면 문서를 써주겠지" 라고 생각했지만 그럴리가. V&V 문서는 레퍼런스 문서가 아니다. 계획과 설계를 작성해야하는데 너무 구체적으로 적으면 영업비밀이 유출될 수 있고, 너무 추상적으로 적으면 내용없는 글이 된다. 에이전트와 적절한 균형을 찾아가는 여정이 필요했다.

그리고 SW V&V에서는 항목(items) 관리가 꽤 큰 업무다. 다행히 Typst는 여기서 정말 강력했다. Source of truth를 YAML 형식으로 기록하여 한 곳에 모아두고 모든 문서가 공통 데이터를 참조하도록 했다. 여기에 각종 식약처 가이드라인, 회의 자료, 검토 의견 등 수많은 컨텍스트들을 에이전트가 활용할 수 있도록 했다.

그래서 (1) 현실의 시스템을 약간 추상화하여 글쓰기, (2) 항목(items)을 한 곳에 모아 YAML로 관리하기, (3) 각종 컨텍스트를 에이전트가 필요할 때 참조할 수 있도록 하기. 이 세 가지를 중심으로 에이전트 스킬을 구성하니 그럭저럭 쓸만해졌다. [이 프로젝트](https://github.com/hyounggyu/swvnv)는 이 작업을 매우 단순하게 만든 예제이다. 현실적으로 Typst를 회사에 도입해서 쓸 수 있는 회사가 몇 없을거라 도움은 안될 수 있지만...

핵심은 전체 구조를 만든 뒤에 계속 스킬을 발전시키는 일이다. 처음 생성한 토큰들은 지극히 한심한 수준이었다(참고로 이 작업에 Codex와 GPT 5.5 medium 모델을 사용했다). 에이전트가 작성한 내용을 계속 검토하면서 스킬을 업데이트해야 의미있는 작업들이 가능했다. [Satya Nadella가 최근 X에 올린 글](https://x.com/satyanadella/status/2066182223213293753?s=20)도 이러한 의미를 담고 있는 것이라고 봤다. 물론 암묵지를 에이전트에 입력하는 직원의 입장은 다를 수 있겠지만.

* 참고: 발표 영상의 이미지 생성은 Codex의 `$Image Gen` 스킬을 쓰고, 음성 생성은 ElevenLabs의 Eleven v3 모델을 사용했다.
