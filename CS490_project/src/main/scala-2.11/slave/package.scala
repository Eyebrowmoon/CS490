
package object slave {
  val MAX_SAMPLE_SIZE = 100000 // 1MB of keys

  abstract class SlaveState
  object SlaveConnectState extends SlaveState
  object SlaveComputeState extends SlaveState
  object SlaveSuccessState extends SlaveState
}
