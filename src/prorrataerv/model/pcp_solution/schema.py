from dataclasses import dataclass
from enum import Enum


class GeneratorProperty(Enum):
    GENERATION = 1, 1, "Generation"
    UNITS_GENERATING = 1, 6, "Units Generating"
    DISPATCHABLE_CAPACITY = 1, 9, "Dispatchable Capacity"
    UNDISPATCHABLE_CAPACITY = 1, 10, "Undispatched Capacity"
    CAPACITY_CURTAILED = 1, 28, "Capacity Curtailed"
    AUXILIARY_USE = 1, 74, "Auxiliary Use"
    PUMP_LOAD = 1, 78, "Pump Load"
    GENERATION_COST = 1, 104, "Generation Cost"
    START_STOP_COST = 1, 105, "Start & Shutdown Cost"
    TOTAL_GENERATION_COST = 1, 112, "Total Generation Cost"
    SRMC = 1, 123, "SRMC"
    MAX_CAPACITY = 1, 200, "Max Capacity"
    UNITS_OUT = 1, 209, "Units Out"
    AVAILABLE_CAPACITY = 1, 219, "Available Capacity"
    PROVISION = 118, 601, "Provision"


class FuelProperty(Enum):
    OFFTAKE = 33, 344, "Offtake"
    SHADOW_PRICE = 33, 361, "Shadow Price"


class StorageProperty(Enum):
    MAX_VOLUME = 80, 478, "Max Volume"
    MIN_VOLUME = 80, 479, "Min Volume"
    INITIAL_VOLUME = 80, 480, "Initial Volume"
    END_VOLUME = 80, 481, "End Volume"
    NATURAL_INFLOW = 80, 492, "Natural Inflow"
    SHADOW_PRICE = 80, 505, "Shadow Price"
    NON_PHYSICAL_INFLOW = 80, 524, "Non-physical Inflow"


class WaterwayProperty(Enum):
    FLOW = 88, 529, "Flow"
    MAX_FLOW = 88, 530, "Max Flow"
    MIN_FLOW = 88, 531, "Min Flow"
    SHADOW_PRICE = 88, 533, "Shadow Price"


class ReserveProperty(Enum):
    PROVISION = 115, 586, "Provision"
    SHORTAGE = 115, 588, "Shortage"
    CLEARED_OFFER_PRICE = 115, 591, "Cleared Offer Price"
    PRICE = 115, 594, "Price"
    AVAILABLE_RESPONSE = 115, 596, "Available Response"


class NodeProperty(Enum):
    LOAD = 245, 1200, "Load"
    GENERATION = 245, 1204, "Generation"
    UNSERVED_ENERGY = 245, 1229, "Unserved Energy"
    LOSSES = 245, 1231, "Losses"
    PRICE = 245, 1233, "Price"
    MARGINAL_LOSS_FACTOR = 245, 1237, "Marginal Loss Factor"
    PHASE_ANGLE = 245, 1239, "Phase Angle"


class LineProperty(Enum):
    FLOW = 264, 1274, "Flow"
    EXPORT_LIMIT = 264, 1277, "Export Limit"
    IMPORT_LIMIT = 264, 1278, "Import Limit"
    HOURS_CONGESTED = 264, 1281, "Hours Congested"
    RENTAL = 264, 1303, "Rental"
    LOSS = 264, 1307, "Loss"
    UNITS_OUT = 264, 1328, "Units Out"


class ConstraintProperty(Enum):
    ACTIVITY = 635, 2642, "Activity"
    SLACK = 635, 2643, "Slack"
    HOURS_BINDING = 635, 2645, "Hours Binding"
    RHS = 635, 2646, "RHS"
    PRICE = 635, 2647, "Price"


class DecisionVariableProperty(Enum):
    Value = 642, 2660, "value"


@dataclass
class QuerySchema:
    GENERATOR = GeneratorProperty
    FUEL = FuelProperty
    STORAGE = StorageProperty
    WATERWAY = WaterwayProperty
    RESERVE = ReserveProperty
    NODE = NodeProperty
    LINE = LineProperty
    CONSTRAINT = ConstraintProperty
    DECISION_VARIABLE = DecisionVariableProperty
