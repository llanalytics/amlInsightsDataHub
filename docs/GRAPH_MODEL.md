# Data Hub Graph Model Specification

Version: `1.0`  
Status: Draft (Implementation Contract)  
Owner: `amlInsightsDataHub`

## 1. Purpose
Define a formal graph model for Data Hub graph construction (NetworkX) and downstream visualization/interaction in amlInsights (Cytoscape.js).

## 2. Scope
This model defines:
- Canonical node types and IDs
- Canonical edge types and direction
- Required and optional attributes
- Surrogate (inferred) nodes based on name/address
- OFAC linkage model
- Account-counterparty transaction flow edges with aggregated values

## 3. Graph Runtime
- Graph library: `networkx` (`MultiDiGraph`)
- Directionality: directed edges
- Multiplicity: multiple edge types allowed between same node pair
- Primary dataset: current records only (`is_current = 1`) unless explicitly noted

## 4. Node Model

## 4.1 Node ID Convention
`<NodeType>:<BusinessKey>`

Examples:
- `Customer:CUST-001`
- `Account:ACCT-00001`
- `CounterpartyAccount:CP-700001`
- `OfacSdn:12345`
- `SurrogateName:6f6d...`

## 4.2 Canonical Node Types
- `Household` (`dh_dim_household.household_key`)
- `Customer` (`dh_dim_customer.customer_key`)
- `AssociatedParty` (`dh_dim_associated_party.associated_party_key`)
- `Account` (`dh_dim_account.account_key`)
- `CounterpartyAccount` (`dh_dim_counterparty_account.counterparty_account_key`)
- `Branch` (`dh_dim_branch.branch_key`)
- `Country` (`dh_dim_country.country_code_2`)
- `Currency` (`dh_dim_currency.currency_code`)
- `TransactionType` (`dh_dim_transaction_type.transaction_type_code`)
- `PanamaNode` (`dh_dim_panama_node.node_id` plus `node_type` attribute)
- `OfacSdn` (`dh_dim_ofac_sdn.sdn_uid`)
- `SurrogateName` (hash of normalized name)
- `SurrogateAddress` (hash of normalized address)
- `SurrogateNameAddress` (hash of normalized name + normalized address)

## 4.3 Required Node Attributes
All node types must include:
- `id` (graph node id)
- `node_type` (canonical type)
- `label` (display value)
- `source_table`
- `is_inferred` (`true` for surrogate nodes; else `false`)
- `as_of_ts` (snapshot timestamp)

## 5. Edge Model

## 5.1 Edge ID Convention
`<src>|<edge_type>|<dst>|<as_of_ts>`

## 5.2 Canonical Edges (Base)
- `Household -> Customer` edge type `HOUSEHOLD_HAS_CUSTOMER` (from `dh_bridge_household_customer`)
- `Customer -> Account` edge type `CUSTOMER_HAS_ACCOUNT` (from `dh_bridge_customer_account`, include `relationship_type`)
- `Customer -> AssociatedParty` edge type `CUSTOMER_HAS_ASSOCIATED_PARTY` (from `dh_bridge_customer_associated_party`)
- `PanamaNode -> PanamaNode` edge type `PANAMA_RELATIONSHIP` (from `dh_bridge_panama_relationship`)

## 5.3 Surrogate Edges (Inferred)
- `Entity -> SurrogateName` edge type `HAS_NAME_SIGNATURE`
- `Entity -> SurrogateAddress` edge type `HAS_ADDRESS_SIGNATURE`
- `Entity -> SurrogateNameAddress` edge type `HAS_NAME_ADDRESS_SIGNATURE`

Supported entity node types:
- `Customer`, `AssociatedParty`, `CounterpartyAccount`, `PanamaNode`, optional `OfacSdn`

Required attributes:
- `is_inferred = true`
- `match_method` (`normalized_exact`, `normalized_composite`)
- `normalization_version`
- `confidence` (0.0-1.0)

## 5.4 OFAC Match Edges (Inferred)
- `Entity -> OfacSdn` edge type `POTENTIAL_OFAC_MATCH`

Required attributes:
- `match_method` (`exact_name`, `normalized_name`, `fuzzy_name`)
- `match_score` (0.0-1.0)
- `is_confirmed` (bool)
- `as_of_date`
- `is_inferred = true`

## 5.5 Transaction Flow Edges (Aggregated)
- `Account -> CounterpartyAccount` edge type `TXN_FLOW_AGG` (single edge per account/counterparty pair)

Derived from `dh_fact_cash` using grouped aggregation by:
- `account_key`
- `counterparty_account_key`
- optional time window
- filtered to external-transfer transaction classifications

Required attributes:
- `total_amount` (sum)
- `txn_count`
- `first_txn_ts`
- `last_txn_ts`
- `inbound_amount`
- `inbound_txn_count`
- `outbound_amount`
- `outbound_txn_count`
- `transaction_type_codes` (list)

Optional attributes:
- `avg_amount`
- `max_amount`
- `currency_code` (single-currency datasets)
- `currency_mix` (multi-currency future)
- `time_window` (`all_time`, `30d`, `90d`, etc.)

## 6. Direction Rules
- Base relationship edges follow business semantic direction as listed above.
- Transaction flow is represented by a single directed edge `Account -> CounterpartyAccount`.
- Inbound vs outbound activity is carried as edge attributes (`inbound_*`, `outbound_*`) rather than separate links.

## 7. Unknown/Sentinel Handling
- Unknown member key: `NA` (preferred sentinel)
- Unknown links should remain valid graph nodes when possible (e.g., `CounterpartyAccount:NA`, `Branch:NA`)
- Unknown links should not be dropped silently; include them with low-confidence semantics where applicable.

## 8. Normalization Rules (Surrogate Nodes)
- Name normalization:
  - trim, uppercase
  - remove punctuation
  - collapse whitespace
  - normalize common suffixes (`INC`, `LLC`, etc.)
- Address normalization:
  - trim, uppercase
  - standardize street/unit/state tokens
  - collapse whitespace
  - include postal and country where present
- Hashing:
  - `sha256(normalized_value)` used for surrogate node key

## 9. Snapshot Contract
Each graph build produces a snapshot:
- `snapshot_id`
- `as_of_ts`
- `node_count`
- `edge_count`
- `model_version` (this spec version)
- `elements` payload compatible with Cytoscape.js

## 9.1 Cytoscape.js Payload Shape
```json
{
  "snapshot_id": "graph_2026-04-21T12:00:00Z",
  "model_version": "1.0",
  "elements": {
    "nodes": [
      { "data": { "id": "Customer:CUST-001", "node_type": "Customer", "label": "Jane Doe" } }
    ],
    "edges": [
      { "data": { "id": "Customer:CUST-001|CUSTOMER_HAS_ACCOUNT|Account:ACCT-1|...", "source": "Customer:CUST-001", "target": "Account:ACCT-1", "edge_type": "CUSTOMER_HAS_ACCOUNT" } }
    ]
  }
}
```

## 10. Minimum Implementation Phases
1. Base entity graph (`Household/Customer/Account/AssociatedParty`)  
2. Transaction flow aggregation edges (`TXN_FLOW_*`)  
3. Surrogate nodes/edges (name/address)  
4. OFAC match edges  
5. Panama overlay and path analytics

## 11. Validation Rules
- All edges must reference existing node IDs in the same snapshot.
- No duplicate edge IDs in a snapshot.
- Required attributes must be present per node/edge type.
- `TXN_FLOW_*` edges must have numeric `total_amount` and integer `txn_count`.

## 12. Non-Goals (Current Version)
- Real-time streaming graph updates
- Probabilistic entity resolution beyond configured matching rules
- Full graph persistence in a dedicated graph database
