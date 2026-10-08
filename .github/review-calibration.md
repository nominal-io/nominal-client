# Checking review quality

When changing the review prompt, try it on a few real PRs before relying on it.
Review the original diff without showing the model later human feedback, then compare:
what useful findings did it miss, and what unnecessary work did it ask for?
Record the model, prompt revision, and reviewed commits in the PR description.

A few examples of the judgment we want:

| Example | Expected review |
| --- | --- |
| A generic layer adds modes and casts where direct code would be substantially clearer. | Ask for simplification and show what disappears. A predicted bug is not required. |
| Two short constructors have different contracts, or a one-caller helper makes an operation clearer. | Keep them; duplication and reuse count alone do not justify a rewrite. |
| A test protects empty-versus-omitted request fields and in-place refresh. | Keep it. A test that only returns its own configured mock value adds no protection. See [#1026](https://github.com/nominal-io/nominal-client/pull/1026). |
| Removing a serialization dependency changes persisted field names. | Check old-format reads, not just new-format round trips. See [#803](https://github.com/nominal-io/nominal-client/pull/803) and its repair in [#823](https://github.com/nominal-io/nominal-client/pull/823). |

These are examples, not another checklist. The [development guides](../docs/development/README.md)
own the conventions. Keep detailed experiment results in the PR, and use subsequent human
reviews to judge whether the prompt improves. A quiet AI review is not evidence by itself.
