# bamdra-user-bind

![Bamdra Animated Logo](./docs/assets/bamdra-logo-animated.svg)

Bamdra 套件中的身份与“活画像”层。

它可以独立运行，也会被 `bamdra-openclaw-memory` 自动补齐。

单独安装：

```bash
openclaw plugins install @bamdra/bamdra-user-bind
```

[English README](./README.md)

## 它做什么

`bamdra-user-bind` 会把渠道里的原始 sender ID 转成稳定用户边界。

同时，它也会逐渐成为用户持续演化的画像层，包括：

- 应该怎么称呼
- 时区
- 语气偏好
- 角色
- 长期用户备注

## 为什么重要

没有身份层时：

- 同一个人可能在不同渠道或会话里碎片化
- 记忆可能挂错边界
- 个性化很难稳定

有了它之后：

- user-aware 记忆会稳定下来
- 个性化会跨 session 持续存在
- 智能体会逐步适应用户的风格和习惯

## 存储模型

- 主存储：
  `~/.openclaw/data/bamdra-user-bind/profiles.sqlite`
- 可编辑 Markdown 镜像：
  `~/.openclaw/data/bamdra-user-bind/profiles/private/{userId}.md`
- 导出目录：
  `~/.openclaw/data/bamdra-user-bind/exports/`

SQLite 是受控主源。

Markdown 镜像则是给人编辑的，让用户画像更像一份活的 per-user 指南，而不是一个无法触达的黑盒。

## 最佳实践

- SQLite 留在本地
- 画像镜像保持私有
- 让人逐步维护画像镜像
- 管理员能力只用于审计、合并、修复和维护

## 架构图

![Bamdra 套件架构图](./docs/assets/architecture-technical-zh.svg)

## 它能解锁什么

与 `bamdra-openclaw-memory` 组合时：

- 记忆会从 session-only 变成真正的 user-aware

与 `bamdra-memory-vector` 组合时：

- 私有笔记既能保持私有，又能影响本地召回

## 仓库地址

- [GitHub 首页](https://github.com/bamdra)
- [仓库地址](https://github.com/bamdra/bamdra-user-bind)
