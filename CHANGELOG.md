# Changelog

All notable changes to hikyaku are listed here, newest first. Versions follow [Semantic Versioning](https://semver.org/). Each new release is added at the top, with notes generated from the commit messages.

# [1.6.0](https://github.com/xavidop/hikyaku/compare/v1.5.1...v1.6.0) (2026-11-13)


### Features

* oauth2 for http poll sources ([#24](https://github.com/xavidop/hikyaku/issues/24)) ([bf936c6](https://github.com/xavidop/hikyaku/commit/bf936c6e4634b43a6a0b6bda3cbe41d7f4b17d1c))

## [1.5.1](https://github.com/xavidop/hikyaku/compare/v1.5.0...v1.5.1) (2026-11-13)


### Bug Fixes

* report filters that cannot be evaluated; hikyaku test answers enrich calls ([#23](https://github.com/xavidop/hikyaku/issues/23)) ([ce2dc17](https://github.com/xavidop/hikyaku/commit/ce2dc17eaff9c61bf12b9db0878ab8f669dede88))

# [1.5.0](https://github.com/xavidop/hikyaku/compare/v1.4.0...v1.5.0) (2026-11-12)


### Features

* hikyaku graph, test and forward ([#22](https://github.com/xavidop/hikyaku/issues/22)) ([8765438](https://github.com/xavidop/hikyaku/commit/87654382d6afc52e8b0220167ef86ed9447a3232))

# [1.4.0](https://github.com/xavidop/hikyaku/compare/v1.3.0...v1.4.0) (2026-11-12)


### Features

* OAuth2, shadow traffic, a schedule source, email and gzip ([#16](https://github.com/xavidop/hikyaku/issues/16)) ([272f22f](https://github.com/xavidop/hikyaku/commit/272f22f7e2e8d6e5ad71d7752857d4e18d8587e2))

# [1.3.0](https://github.com/xavidop/hikyaku/compare/v1.2.0...v1.3.0) (2026-11-11)


### Features

* webhook presets, alerts and the enrich transform ([#15](https://github.com/xavidop/hikyaku/issues/15)) ([955c78d](https://github.com/xavidop/hikyaku/commit/955c78d1ced04299ab731aecf4dfa48050c1a3d8))

# [1.2.0](https://github.com/xavidop/hikyaku/compare/v1.1.0...v1.2.0) (2026-11-11)


### Features

* **engine:** deliver later with deliver_after and deliver_at ([5c5aa7e](https://github.com/xavidop/hikyaku/commit/5c5aa7ec992c8a370f46934abda7484ba0dae418))
* **engine:** rate limit a destination ([8d47df7](https://github.com/xavidop/hikyaku/commit/8d47df7fc5aed1e845d99630fa02ed58de39c746))
* **http:** sign outgoing requests with an HMAC of the body ([4996ebf](https://github.com/xavidop/hikyaku/commit/4996ebfbf65180b4daab25fe3caebe83686b44b9))
* **transform:** redact fields of a JSON body ([e7568a4](https://github.com/xavidop/hikyaku/commit/e7568a4f39b10672aeed2756b426b4dd083fb7ed))
* **transform:** send a body validate refused to on_invalid destinations ([3a901d1](https://github.com/xavidop/hikyaku/commit/3a901d13c5d7d8b68b47e127a8b86ca380ab3597))

# [1.1.0](https://github.com/xavidop/hikyaku/compare/v1.0.1...v1.1.0) (2026-11-10)


### Features

* **transform:** validate a body against a JSON Schema ([#9](https://github.com/xavidop/hikyaku/issues/9)) ([b80b79c](https://github.com/xavidop/hikyaku/commit/b80b79c39fa52e3e903cfe5b8ea7350586a13ce3))

## [1.0.1](https://github.com/xavidop/hikyaku/compare/v1.0.0...v1.0.1) (2026-11-10)


### Bug Fixes

* **amqp:** report a reconnected source healthy as soon as it consumes ([359d8c1](https://github.com/xavidop/hikyaku/commit/359d8c1fd60771eea003296b85723dd37716f152))
* **nats:** give a stopped jetstream source's messages back only once its consumer is closed ([fc2a898](https://github.com/xavidop/hikyaku/commit/fc2a898115da00f14eea715e219040109cbfd442))

# 1.0.0 (2026-11-10)


### Features

* initial commit ([9d6857c](https://github.com/xavidop/hikyaku/commit/9d6857cfd9f30bb6312cd0a5bd5c01db0c9287cf))
