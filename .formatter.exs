locals_without_parens = [
  # VerifiedPubsub.Dsl entities
  topic: 2,
  topic: 3,
  message: 1,
  message: 2,
  field: 2,
  field: 3,
  # VerifiedPubsub.Subscriber macros
  handle_message: 5,
  ignore_message: 2
]

[
  inputs: ["{mix,.formatter}.exs", "{config,lib,test}/**/*.{ex,exs}"],
  locals_without_parens: locals_without_parens,
  export: [locals_without_parens: locals_without_parens]
]
