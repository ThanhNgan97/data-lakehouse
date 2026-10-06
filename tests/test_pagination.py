from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(
    0,
    str(ROOT / "lakehouse" / "spark"),
)

from pagination import (
    CursorPagination,
    OffsetLimitPagination,
    PageLimitPagination,
    PaginationDecision,
)


def payload(
    *,
    has_more,
    next_cursor=None,
):
    return {
        "pagination": {
            "has_more": has_more,
            "next_cursor": next_cursor,
        }
    }


class PaginationStrategyTests(
    unittest.TestCase
):

    def test_cursor_pagination_state_and_params(
        self,
    ):
        strategy = CursorPagination()

        state = strategy.initial_state()

        self.assertIsNone(state)

        self.assertEqual(
            strategy.request_params(
                state,
                limit=100,
            ),
            {
                "limit": "100",
                "cursor": None,
            },
        )

        decision = strategy.advance(
            state,
            payload(
                has_more=True,
                next_cursor="cursor-2",
            ),
            limit=100,
        )

        self.assertEqual(
            decision,
            PaginationDecision(
                done=False,
                next_state="cursor-2",
            ),
        )

        self.assertEqual(
            strategy.request_params(
                decision.next_state,
                limit=100,
            ),
            {
                "limit": "100",
                "cursor": "cursor-2",
            },
        )

        final_decision = strategy.advance(
            decision.next_state,
            payload(
                has_more=False,
            ),
            limit=100,
        )

        self.assertEqual(
            final_decision,
            PaginationDecision(
                done=True,
            ),
        )

    def test_cursor_requires_next_cursor_when_more_data(
        self,
    ):
        strategy = CursorPagination()

        with self.assertRaisesRegex(
            ValueError,
            "next_cursor is missing",
        ):
            strategy.advance(
                None,
                payload(
                    has_more=True,
                    next_cursor=None,
                ),
                limit=100,
            )

    def test_page_limit_pagination_advances_one_page(
        self,
    ):
        strategy = PageLimitPagination()

        state = strategy.initial_state()

        self.assertEqual(
            state,
            1,
        )

        self.assertEqual(
            strategy.request_params(
                state,
                limit=50,
            ),
            {
                "page": "1",
                "limit": "50",
            },
        )

        decision = strategy.advance(
            state,
            payload(
                has_more=True,
            ),
            limit=50,
        )

        self.assertEqual(
            decision,
            PaginationDecision(
                done=False,
                next_state=2,
            ),
        )

        final_decision = strategy.advance(
            decision.next_state,
            payload(
                has_more=False,
            ),
            limit=50,
        )

        self.assertEqual(
            final_decision,
            PaginationDecision(
                done=True,
            ),
        )

    def test_offset_limit_pagination_advances_by_limit(
        self,
    ):
        strategy = OffsetLimitPagination()

        state = strategy.initial_state()

        self.assertEqual(
            state,
            0,
        )

        self.assertEqual(
            strategy.request_params(
                state,
                limit=50,
            ),
            {
                "offset": "0",
                "limit": "50",
            },
        )

        decision = strategy.advance(
            state,
            payload(
                has_more=True,
            ),
            limit=50,
        )

        self.assertEqual(
            decision,
            PaginationDecision(
                done=False,
                next_state=50,
            ),
        )

        next_decision = strategy.advance(
            decision.next_state,
            payload(
                has_more=True,
            ),
            limit=50,
        )

        self.assertEqual(
            next_decision.next_state,
            100,
        )

    def test_has_more_must_be_boolean(
        self,
    ):
        strategy = PageLimitPagination()

        with self.assertRaisesRegex(
            ValueError,
            "has_more must be a boolean",
        ):
            strategy.advance(
                1,
                {
                    "pagination": {
                        "has_more": "yes",
                    }
                },
                limit=100,
            )


if __name__ == "__main__":
    unittest.main()
