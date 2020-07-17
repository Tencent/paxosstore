#include "src/entry_state.h"

#include "gtest/gtest.h"

class EntryStateTest : public testing::Test {
 public:
  virtual void SetUp() override {}
  virtual void TearDown() override {}

 protected:
  const uint64_t entity_id_ = 123456789;
  const uint64_t entry_ = 100;
  const int acceptor_num1_ = 1;
  const int acceptor_num3_ = 3;
  const int acceptor_num5_ = 5;
  const int local_acceptor_id_ = 0;
  const int peer_acceptor_id1_ = 1;
  const int peer_acceptor_id2_ = 2;

  const uint64_t uuid_ = 120701;
  const std::vector<uint64_t> uuids_ = {120701, 120702};
  const uint64_t value_id_ = 10;
  const std::string value_ = "Hello World";
};

typedef certain::EntryStateMachine Cesm;

TEST_F(EntryStateTest, IsValidRecord) {
  certain::EntryRecord record;
  ASSERT_TRUE(Cesm::IsValidRecord(entity_id_, entry_, record));

  record.set_prepared_num(1);
  ASSERT_FALSE(Cesm::IsValidRecord(entity_id_, entry_, record));

  record.set_promised_num(2);
  ASSERT_TRUE(Cesm::IsValidRecord(entity_id_, entry_, record));

  record.set_accepted_num(2);
  ASSERT_FALSE(Cesm::IsValidRecord(entity_id_, entry_, record));

  record.set_value_id(100);
  ASSERT_TRUE(Cesm::IsValidRecord(entity_id_, entry_, record));

  record.set_accepted_num(3);
  ASSERT_FALSE(Cesm::IsValidRecord(entity_id_, entry_, record));
}

certain::EntryRecord MakeEntryRecord(uint32_t prepared_num,
                                     uint32_t promised_num,
                                     uint32_t accepted_num, uint64_t value_id,
                                     std::string value, uint64_t uuid,
                                     bool chosen, bool has_value_id_only) {
  certain::EntryRecord record;
  record.set_prepared_num(prepared_num);
  record.set_promised_num(promised_num);
  record.set_accepted_num(accepted_num);
  record.set_value_id(value_id);
  record.set_value(value);
  if (uuid > 0) {
    record.add_uuids(uuid);
  }
  record.set_chosen(chosen);
  record.set_has_value_id_only(has_value_id_only);
  return record;
}

TEST_F(EntryStateTest, IsRecordNewer) {
  auto oldr = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  auto newr = MakeEntryRecord(2, 1, 0, 0, "", 0, false, false);
  ASSERT_TRUE(Cesm::IsRecordNewer(oldr, newr));
}

TEST_F(EntryStateTest, Update1) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num1_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);
  auto prepare_record = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, prepare_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kMajorityPromise);

  auto accept_record =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, PreAuth1) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num1_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);
  auto record =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, Update3) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num3_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);
  auto prepare_record = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, prepare_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, prepare_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kMajorityPromise);

  auto accept_record =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kAcceptLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, PreAuth3) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num3_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);
  auto accept_record =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kAcceptLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, Update5) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num5_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);
  auto prepare_record = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, prepare_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, prepare_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id2_, prepare_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kMajorityPromise);

  auto accept_record =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kAcceptLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kAcceptLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id2_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, PreAuth5) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num5_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);

  auto accept_record =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kAcceptLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kAcceptLocal);
  ASSERT_EQ(m.Update(peer_acceptor_id2_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, NotProposerUpdate) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num3_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);

  auto prepare_record = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, prepare_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseRemote);

  auto accept_record =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, accept_record), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, Concurrent) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num3_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);

  auto prepare_record1 = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, prepare_record1), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseRemote);

  auto prepare_record2 = MakeEntryRecord(2, 2, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(peer_acceptor_id2_, prepare_record2), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseRemote);

  auto accept_record1 =
      MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(peer_acceptor_id1_, accept_record1), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseRemote);

  auto accept_record2 =
      MakeEntryRecord(2, 2, 2, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(peer_acceptor_id2_, accept_record2), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, Concurrent2) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num3_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);

  auto prepare_record1 = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, prepare_record1), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kPromiseLocal);

  auto accept_record2 =
      MakeEntryRecord(2, 2, 2, value_id_, value_, uuid_, false, false);
  ASSERT_EQ(m.Update(peer_acceptor_id2_, accept_record2), 0);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, AcceptRemote) {
  {
    certain::EntryStateMachine m3(entity_id_, entry_, acceptor_num3_,
                                  local_acceptor_id_);
    auto accept_record =
        MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
    ASSERT_EQ(m3.Update(peer_acceptor_id1_, accept_record), 0);
    // kAcceptRemote -> kChosen
    ASSERT_EQ(m3.entry_state(), certain::EntryState::kChosen);
  }
  {
    certain::EntryStateMachine m5(entity_id_, entry_, acceptor_num5_,
                                  local_acceptor_id_);
    auto accept_record =
        MakeEntryRecord(1, 1, 1, value_id_, value_, uuid_, false, false);
    ASSERT_EQ(m5.Update(peer_acceptor_id1_, accept_record), 0);
    ASSERT_EQ(m5.entry_state(), certain::EntryState::kAcceptRemote);
  }
}

TEST_F(EntryStateTest, PromiseMultiTime) {
  certain::EntryStateMachine m3(entity_id_, entry_, acceptor_num3_,
                                local_acceptor_id_);
  ASSERT_EQ(m3.Promise(), 0);
  auto record = m3.GetEntryRecord(local_acceptor_id_);
  ASSERT_EQ(record.promised_num(), acceptor_num3_ + local_acceptor_id_ + 1);

  ASSERT_EQ(m3.Promise(), 0);
  record = m3.GetEntryRecord(local_acceptor_id_);
  ASSERT_EQ(record.promised_num(), acceptor_num3_ * 2 + local_acceptor_id_ + 1);

  ASSERT_EQ(m3.Promise(), 0);
  record = m3.GetEntryRecord(local_acceptor_id_);
  ASSERT_EQ(record.promised_num(), acceptor_num3_ * 3 + local_acceptor_id_ + 1);
}

TEST_F(EntryStateTest, PromiseAndAccept) {
  bool prepared_value_accepted = false;
  certain::EntryStateMachine m3(entity_id_, entry_, acceptor_num3_,
                                local_acceptor_id_);
  ASSERT_EQ(m3.Promise(), 0);
  auto record = m3.GetEntryRecord(local_acceptor_id_);
  // kNormal -> kPromiseLocal
  ASSERT_EQ(record.promised_num(), acceptor_num3_ + local_acceptor_id_ + 1);
  ASSERT_EQ(record.accepted_num(), 0);
  ASSERT_EQ(m3.entry_state(), certain::EntryState::kPromiseLocal);
  ASSERT_EQ(m3.Update(local_acceptor_id_, record), 0);
  ASSERT_EQ(m3.entry_state(), certain::EntryState::kPromiseLocal);
  // kPromiseLocal -> kMajorityPromise
  ASSERT_EQ(m3.Update(peer_acceptor_id2_, record), 0);
  ASSERT_EQ(m3.entry_state(), certain::EntryState::kMajorityPromise);
  // kMajorityPromise -> kAcceptLocal
  ASSERT_EQ(m3.Accept(value_, value_id_, uuids_, &prepared_value_accepted), 0);
  ASSERT_TRUE(prepared_value_accepted);
  ASSERT_EQ(m3.entry_state(), certain::EntryState::kAcceptLocal);
  // kAcceptLocal -> kChosen
  record = m3.GetEntryRecord(local_acceptor_id_);
  ASSERT_EQ(record.accepted_num(), acceptor_num3_ + local_acceptor_id_ + 1);
  ASSERT_EQ(m3.Update(peer_acceptor_id2_, record), 0);
  ASSERT_EQ(m3.entry_state(), certain::EntryState::kChosen);
}

TEST_F(EntryStateTest, ReadRoutine) {
  certain::EntryStateMachine m3(entity_id_, entry_, acceptor_num3_,
                                local_acceptor_id_);
  ASSERT_TRUE(m3.IsLocalEmpty());
  m3.ResetEmptyFlags();
  ASSERT_FALSE(m3.IsMajorityEmpty());
  m3.SetEmptyFlag(peer_acceptor_id1_);
  ASSERT_TRUE(m3.IsMajorityEmpty());
  // Reset and redo again.
  m3.ResetEmptyFlags();
  ASSERT_FALSE(m3.IsMajorityEmpty());
  m3.SetEmptyFlag(peer_acceptor_id1_);
  ASSERT_TRUE(m3.IsMajorityEmpty());
}

TEST_F(EntryStateTest, CalcSize) {
  certain::EntryStateMachine m(entity_id_, entry_, acceptor_num1_,
                               local_acceptor_id_);
  ASSERT_EQ(m.entry_state(), certain::EntryState::kNormal);
  auto size1 = m.CalcSize();

  auto record = MakeEntryRecord(1, 1, 0, 0, "", 0, false, false);
  ASSERT_EQ(m.Update(local_acceptor_id_, record), 0);
  auto size2 = m.CalcSize();
  ASSERT_EQ(size1 + record.ByteSize(), size2);
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
