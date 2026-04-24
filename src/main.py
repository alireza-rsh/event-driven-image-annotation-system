from messaging.redis_broker import RedisBroker
from services.cli_service import CLIService


def main():
    broker = RedisBroker()

    query_service = QueryService(broker)
    cli_service = CLIService(broker)

    # In a real deployment these would be separate processes/services.
    # For now, start both listeners.
    import threading

    threading.Thread(target=query_service.start, daemon=True).start()
    cli_service.run_interactive()


if __name__ == "__main__":
    main()