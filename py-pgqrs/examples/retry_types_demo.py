"""
Demo of retry-related types in pgqrs Python bindings.

This example demonstrates the retry policy types that will be used
once the core retry functionality is implemented in the Rust layer.
"""

from pgqrs import BackoffStrategy, StepRetryPolicy


def main():
    """Demonstrate retry policy construction."""

    # Default retry policy: 3 attempts with exponential backoff + jitter
    default_policy = StepRetryPolicy()
    print(f"Default policy: {default_policy}")
    print(f"  Max attempts: {default_policy.max_attempts}")

    # Fixed backoff strategy
    fixed_backoff = BackoffStrategy.fixed(delay_seconds=5)
    fixed_policy = StepRetryPolicy(max_attempts=5, backoff=fixed_backoff)
    print(f"\nFixed backoff policy: {fixed_policy}")

    # Exponential backoff strategy
    exp_backoff = BackoffStrategy.exponential(base_seconds=2, max_seconds=60)
    exp_policy = StepRetryPolicy(max_attempts=10, backoff=exp_backoff)
    print(f"\nExponential backoff policy: {exp_policy}")

    # Exponential with jitter (recommended for production)
    jitter_backoff = BackoffStrategy.exponential_with_jitter(
        base_seconds=1, max_seconds=120
    )
    jitter_policy = StepRetryPolicy(max_attempts=7, backoff=jitter_backoff)
    print(f"\nJitter backoff policy: {jitter_policy}")

    print("\n" + "=" * 60)
    print("These types are ready to use once core retry logic is merged.")
    print("=" * 60)


if __name__ == "__main__":
    main()
