
package object slave {
  val MAX_SAMPLE_SIZE = 1000000 // about 1MB of keys

  val slavePort = 24925

  abstract class SlaveState
  object SlaveConnectState extends SlaveState
  object SlaveComputeState extends SlaveState
  object SlaveSuccessState extends SlaveState
}
