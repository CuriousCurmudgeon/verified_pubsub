locals_without_parens = [
  # VerifiedPubSub.Dsl entities
  topic: 2,
  topic: 3,
  message: 1,
  message: 2,
  field: 2,
  field: 3,
  # VerifiedPubSub.Subscriber macros
  handle_message: 5,
  handle_message: 6,
  ignore_message: 2
]

[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
