import unittest

import itc

def equal(a, b):
    return itc.leq(a, b) and itc.leq(b, a)

def smaller(a, b):
    return itc.leq(a, b) and not itc.leq(b, a)

def larger(a, b):
    return not itc.leq(a, b) and itc.leq(b, a)

def clash(a, b):
    return not equal(a, b) and not smaller(a, b) and not larger(a, b)

def add_events(num, clock):
    return reduce(lambda x, y: itc.event(x), xrange(num), clock)

class demo(unittest.TestCase):

    def setUp(self):
        self.A0 = itc.seed()
        self.A1, self.B0 = itc.fork(self.A0)
        self.A2 = itc.event(self.A1)
        self.B1 = itc.event(self.B0)
        self.A3, self.C0 = itc.fork(self.A2)
        self.B2 = itc.event(self.B1)
        self.A4 = itc.event(self.A3)
        self.BC0 = itc.join(self.B2, self.C0)
        self.BC1, self.D0 = itc.fork(self.BC0)
        self.ABC0 = itc.join(self.A4, self.BC1)
        self.ABC1 = itc.event(self.ABC0)

    def test_demo(self):
        # base: 1, added: 0
        self.assertEqual(self.A0, (1,0))
        # 1/2 left, added: 0
        self.assertEqual(self.A1, ((1,0), 0))
        # 1/2 left, added: base 0, left 1, right 0
        self.assertEqual(self.A2, ((1,0), (0,1,0)))
        # 1/4 left, added: base 0, left 1, right 0
        self.assertEqual(self.A3, (((1,0),0),(0,1,0)))
        # 1/4 left, added: base 0, left 1+1/2(left), right 0, left 1
        self.assertEqual(self.A4, (((1,0),0),(0,(1,1,0),0)))
        # 1/2 right, added: 0
        self.assertEqual(self.B0, ((0,1), 0))
        # 1/2 right, added: base 0, left 0, right 1
        self.assertEqual(self.B1, ((0,1), (0,0,1)))
        # 1/2 right, added: base 0, left 0, right 2
        self.assertEqual(self.B2, ((0,1), (0,0,2)))
        # 1/4 left, added: base 0, left 1, right 0
        self.assertEqual(self.C0, (((0,1),0),(0,1,0)))
        # 3/4 right, added: base 1, left 0, right +1 (2)
        self.assertEqual(self.BC0,(((0,1),1),(1,0,1)))
        # fork, keep left BC0, added: base 1, left 0, right 1
        self.assertEqual(self.BC1, (((0,1),0),(1,0,1)))
        # fork, keep 1/2 right, added: base 1, left 0, right 1
        self.assertEqual(self.D0, ((0,1),(1,0,1)))
        # merge left for 1/2, base 1, left 1/2l, right 1
        self.assertEqual(self.ABC0, ((1,0),(1,(0,1,0),1)))
        # fill gap, 1/2 left, added: base 2
        self.assertEqual(self.ABC1, ((1,0),2))

class version_vector(unittest.TestCase):
    def setUp(self):
        self.A0, self.Tmp0 = itc.fork(itc.seed())
        self.B0, self.Tmp1 = itc.fork(self.Tmp0)
        self.C0, self.D0   = itc.fork(self.Tmp1)
        self.B1 = itc.event(self.B0)
        self.D1 = itc.event(self.D0)
        self.E0 = itc.join(self.B1,self.D1)
        self.B2 = itc.join(self.B1, itc.peek(self.D1))
        self.D2 = itc.join(self.D1, itc.peek(self.B1))

    def test_comparisons(self):
        self.assertTrue(equal(self.A0, self.B0))
        self.assertTrue(equal(self.A0, self.C0))
        self.assertTrue(equal(self.A0, self.D0))
        self.assertTrue(equal(self.B0, self.C0))
        self.assertTrue(equal(self.B0, self.D0))
        self.assertTrue(equal(self.C0, self.D0))

    def test_clash(self):
        self.assertFalse(itc.leq(self.B1, self.D1))
        self.assertFalse(itc.leq(self.D1, self.B1))

    def test_merge(self):
        self.assertTrue(equal(self.B2,self.D2))
        self.assertTrue(equal(self.B2,self.E0))
        self.assertTrue(equal(self.D2,self.E0))

class master_to_replicas(unittest.TestCase):
    def setUp(self):
        # 3 cluster: 1 master, two replicas (A,B)
        self.Master0, self.ReplicaBase = itc.fork(itc.seed())
        self.ReplicaA0, self.ReplicaB0 = itc.fork(self.ReplicaBase)
        # Simulate 3 unreplicated writes
        self.Master1 = itc.event(self.Master0)
        self.Master2 = itc.event(self.Master1)
        self.Master3 = itc.event(self.Master2)
        # Replicate them in order from peek and it should work
        self.ReplicaA1 = itc.join(self.ReplicaA0, itc.peek(self.Master1))
        self.ReplicaA2 = itc.join(self.ReplicaA1, itc.peek(self.Master2))
        self.ReplicaA3 = itc.join(self.ReplicaA2, itc.peek(self.Master3))
        itc.join(self.Master3,self.ReplicaA3)
        itc.join(self.Master3,self.ReplicaA2)
        itc.join(self.Master3,self.ReplicaA1)
        self.ReplicaB1 = itc.join(self.ReplicaB0, itc.peek(self.ReplicaA3))

    def test_replication(self):
        # Replication (peek) and merging (join) should work with the base
        # state
        self.assertTrue(equal(itc.peek(self.Master0), itc.peek(self.ReplicaA0)))
        self.assertTrue(equal(itc.peek(self.Master0), itc.join(self.ReplicaA0,self.Master0)))
        self.assertTrue(equal(itc.peek(self.Master0),
                      itc.join(self.ReplicaA0,itc.peek(self.Master0))))

    def test_peek_rep(self):
        # Replicate them in order from peek and it should work
        self.assertTrue(equal(itc.peek(self.Master1), itc.peek(self.ReplicaA1)))
        self.assertTrue(equal(itc.peek(self.Master2), itc.peek(self.ReplicaA2)))
        self.assertTrue(equal(itc.peek(self.Master3), itc.peek(self.ReplicaA3)))
        self.assertTrue(smaller(self.Master2,self.ReplicaA3))
        self.assertTrue(smaller(self.Master1,self.ReplicaA3))
        self.assertTrue(smaller(self.Master0,self.ReplicaA3))

    def test_out_of_order_rep(self):
        # Out of order replications for other replica
        # from possibly many source
        self.assertTrue(smaller(self.ReplicaB0,self.ReplicaA3))
        self.assertTrue(larger(self.ReplicaA3,self.ReplicaB0))
        self.assertTrue(equal(self.ReplicaB1, self.ReplicaA3))
        self.assertTrue(equal(self.ReplicaB1, self.Master3))
        self.assertFalse(smaller(self.ReplicaB1, self.Master3))
        self.assertFalse(larger(self.ReplicaB1, self.Master3))
        self.assertFalse(smaller(self.Master3, self.ReplicaB1))
        self.assertFalse(larger(self.Master3, self.ReplicaB1))

class MasterToMaster(unittest.TestCase):
    def testm2m(self):
        # 3 cluster: 1 master, two replicas (A,B)
        MasterTmp, MasterB0 = itc.fork(itc.seed())
        MasterB1, MasterC0 = itc.fork(MasterB0)
        MasterA0, MasterD0 = itc.fork(MasterTmp)
        MasterA1 = add_events(3, MasterA0)
        MasterB2 = add_events(2, MasterB1)
        MasterC1 = add_events(1, MasterC0)
        MasterB3 = itc.join(MasterB2, itc.peek(MasterC1))
        MasterC2 = itc.join(MasterC1, itc.peek(MasterB2))

         # Concurrent entries can't work, get invalid
        self.assertTrue(clash(MasterA1, MasterB2))
        self.assertTrue(clash(MasterB2, MasterC1))

        # Merges from peek / join work, and supercede the previous values
        self.assertTrue(smaller(MasterC1,
                        itc.join(MasterC1, itc.peek(MasterB2))))
        self.assertTrue(smaller(MasterC1, itc.join(MasterC1, MasterB2)))
        self.assertTrue(smaller(MasterC1,
                        itc.join(MasterC1, itc.peek(MasterA1))))
        self.assertTrue(smaller(MasterC1, itc.join(MasterC1, MasterA1)))
        self.assertTrue(smaller(MasterB2,
                        itc.join(MasterC1, itc.peek(MasterB2))))
        self.assertTrue(smaller(MasterB2, itc.join(MasterC1, MasterB2)))
        self.assertTrue(smaller(MasterA1,
                        itc.join(MasterC1, itc.peek(MasterA1))))
        self.assertTrue(smaller(MasterA1, itc.join(MasterC1, MasterA1)))

        # Both merged entries are equal to each other, and still clash
        # with conflicting ones, but are bigger than sane ones.
        self.assertTrue(equal(MasterB3, MasterC2))
        self.assertTrue(clash(MasterA1, MasterB3))
        self.assertTrue(clash(MasterA1, MasterC2))
        self.assertTrue(smaller(MasterD0, MasterA1))
        self.assertTrue(smaller(MasterD0, MasterB3))
        self.assertTrue(smaller(MasterD0, MasterC2))

class PrivateMethodsTests(unittest.TestCase):

    def test_normal_event_form(self):
        self.assertTrue(itc.normal_event((2,1,1))==3)
        self.assertTrue(itc.normal_event((2,(2,1,0),3))==(4,(0,1,0),1))

    def test_split(self):
        self.assertTrue(itc.split(0)==(0,0))
        self.assertTrue(itc.split(1)==((1,0),(0,1)))
        self.assertTrue(itc.split((0,1))==((0,(1,0)),(0,(0,1))))
        self.assertTrue(itc.split((1,0))==(((1,0),0),((0,1),0)))
        self.assertTrue(itc.split((1,1))==((1,0),(0,1)))