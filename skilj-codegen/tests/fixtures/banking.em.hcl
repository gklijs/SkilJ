# The banking bounded context of skilj-demo (skilj-demo/src/banking.rs) as an
# Event Modeling HCL model (https://event-modeling-hcl.github.io), written with
# the naming convention of docs/event-modeling-hcl.md. banking.emhcl.json is
# what `emhcl export` (v0.17.0) writes for it; tests/emhcl_import.rs turns that
# into a .skilj.toml.

board {
  workflows = [
    workflow.deposit_money,
    workflow.withdraw_money,
    workflow.account_balance,
  ]
}

actor "account_holder" {
  title         = "Account holder"
  auth_required = true
}

bounded_context "banking" {
  title = "Banking"

  field_type "account_id" {
    type         = "String"
    id_attribute = true
    example      = "acc-1"
  }

  field_type "amount" {
    type    = "Int"
    example = 100
  }

  event "money_deposited" {
    title  = "Money Deposited"
    fields = [field_type.account_id, field_type.amount]
  }

  event "money_withdrawn" {
    title  = "Money Withdrawn"
    fields = [field_type.account_id, field_type.amount]
  }
}

state_change "deposit_money" {
  title = "Deposit money"

  screen "deposit_money_request" {
    kind         = "api"
    api_endpoint = "POST /v1/commands/trigger"
    actor        = actor.account_holder
    to           = [command.deposit_money]
  }

  command "deposit_money" {
    title  = "Deposit Money"
    fields = [field_type.banking.account_id, field_type.banking.amount]
    to     = [event.banking.money_deposited]
  }

  scenario "deposit_accepted" {
    case = "happy_path"

    when {
      examples = [{ account_id = "acc-1", amount = 100 }]
      command  = command.deposit_money
    }

    then {
      examples = [{ account_id = "acc-1", amount = 100 }]
      event    = event.banking.money_deposited
    }
  }

  scenario "deposit_not_positive" {
    case = "refused_input"

    when {
      examples = [{ account_id = "acc-1", amount = 0 }]
      command  = command.deposit_money
    }

    then {
      error = "invalid_amount: deposit amount must be positive"
    }
  }
}

state_change "withdraw_money" {
  title = "Withdraw money"

  screen "withdraw_money_request" {
    kind         = "api"
    api_endpoint = "POST /v1/commands/trigger"
    actor        = actor.account_holder
    to           = [command.withdraw_money]
  }

  command "withdraw_money" {
    title  = "Withdraw Money"
    fields = [field_type.banking.account_id, field_type.banking.amount]
    to     = [event.banking.money_withdrawn]
  }

  scenario "withdrawal_within_balance" {
    case = "happy_path"

    given {
      examples = [{ account_id = "acc-1", amount = 100 }]
      event    = event.banking.money_deposited
    }

    when {
      examples = [{ account_id = "acc-1", amount = 40 }]
      command  = command.withdraw_money
    }

    then {
      examples = [{ account_id = "acc-1", amount = 40 }]
      event    = event.banking.money_withdrawn
    }
  }

  scenario "withdrawal_over_balance" {
    case = "limit"

    given {
      examples = [{ account_id = "acc-1", amount = 100 }]
      event    = event.banking.money_deposited
    }

    when {
      examples = [{ account_id = "acc-1", amount = 150 }]
      command  = command.withdraw_money
    }

    then {
      error = "insufficient_funds: withdrawal exceeds the balance"
    }
  }

  scenario "withdrawal_not_positive" {
    case = "refused_input"

    when {
      examples = [{ account_id = "acc-1", amount = 0 }]
      command  = command.withdraw_money
    }

    then {
      error = "invalid_amount: withdrawal amount must be positive"
    }
  }
}

state_view "account_balance" {
  title = "Account balance"

  readmodel "account_balance" {
    title    = "Account Balance"
    question = "What is the balance of this account?"
    from     = [event.banking.money_deposited, event.banking.money_withdrawn]

    field "account_id" {
      type = field_type.banking.account_id
    }

    field "balance" {
      type    = "Int"
      example = 60
    }
  }

  scenario "balance_after_deposit_and_withdrawal" {
    case = "change"

    given {
      examples = [{ account_id = "acc-1", amount = 100 }]
      event    = event.banking.money_deposited
    }

    given {
      examples = [{ account_id = "acc-1", amount = 40 }]
      event    = event.banking.money_withdrawn
    }

    then {
      examples  = [{ account_id = "acc-1", balance = 60 }]
      readmodel = readmodel.account_balance
    }
  }
}
