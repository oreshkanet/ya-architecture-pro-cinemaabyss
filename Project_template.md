## Изучите [README.md](README.md) файл и структуру проекта.

## Задание 1

**Описание доменов:**

- Домен: Управление пользователями:
  - Контекст: Авторизация и управление профилем
  - Контекст: Хранение данных пользователей

- Домен: Управление каталогом фильмов:
  - Контекст: Хранение метаданных фильмов
  - Контекст: Администрирование каталога и просмотр каталога, загрузка и просмотр фильмов, подключение онлайн-кинотеатров
  - Контекст: Хранение загруженных фильмов
  - Контекст: Интеграция с видео-хостингами

- Домен: Управление платежами:
  - Контекст: Проведение платежей
  - Контекст: Хранение состояния проведения платежей

- Домен: Управление подписками:
  - Контекст: Управление подписками пользователей
  - Контекст: Хранение информации о действующих подписках пользователей и их истории

- Домен: Рекомендательная система:
  - Контекст: Получение данных по рекомендациям от внешней системы
  - Контекст: Асинхронная обработка рекомендаций

**Архитектура TOBE:**

[CinemaAbyss_Container](./diagrams/container/CinemaAbyss_Container.png)


## Задание 2

### 1. Proxy

Реализован Proxy-сервис:
[main.go](./src/microservices/proxy/main.go)

### 2. Kafka

Реализован MVP сервис events:
[main.go](./src/microservices/events/main.go)


Проведены тесты для проверки работы Proxy-сервиса и Events-сервиса:

![test](./assets/test_event1.png)
![test](./assets/test_event2.png)
![test](./assets/test_event3.png)

При запуске сервиса Events могут происходит ошибки подключения консьюмеров к Kafka. Возможно, Kafka не успела развернуться и инициализировать нужные топики. Поэтому реализован ретрай подключений для консьюмеров: 
![test](./assets/test_event4.png)

Сообщения в топиках Kafka:

![Test Kafka](./assets/test_kafka1.png)
![Test Kafka](./assets/test_kafka2.png)
![Test Kafka](./assets/test_kafka3.png)

## Задание 3

### CI/CD

Доработан деплой новых сервисов proxy и events в [docker-build-push.yml](./.github/workflows//docker-build-push.yml)

В результате появились "зеленая" сборка и "зеленые" тесты:

![Test CI/CD](./assets/test_cicd1.png)

### Proxy в Kubernetes

Создан токен для k8s:

![K8S token](./assets/k8s_token.png)

Список развёрнутых подов:

![K8S pods](./assets/k8s_pods.png)

> В kafka.yaml добавлен сервис UI, а так же под инициализации для создания нужных топиков. Иначе поды Kafka постоянно перезапускались.

Добавлен ingress:

![K8S ingress](./assets/k8s_ingress.png)

Результат вывода https://cinemaabyss.example.com/api/movies (браузер):

![K8S movies](./assets/k8s_movies.png)

Результаты теста k8s:

![K8S test](./assets/k8s_test1.png)
![K8S test](./assets/k8s_test2.png)
![K8S test](./assets/k8s_test3.png)

## Задание 4

Результат развёртывания HELM:

![HELM deploy](./assets/helm_deploy.png)

Результат запроса https://cinemaabyss.example.com/api/movies:

![HELM getdata](./assets/helm_getdata.png)

Результаты теста:

![HELM test](./assets/helm_test.png)

# Задание 5

В k8s развёрнут Istio и Circuit Breaker, выполнено тестирование с использованием Fortio.

Результаты работы circuit breaker'а:

![circuit breaker](./assets/cb_test1.png)
![circuit breaker](./assets/cb_test2.png)
![circuit breaker](./assets/cb_test3.png)
