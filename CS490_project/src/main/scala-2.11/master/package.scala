package object master {
  abstract class MasterState
  object MasterInitState extends MasterState
  object MasterSampleState extends MasterState
  object MasterComputeState extends MasterState
  object MasterSuccessState extends MasterState
}
