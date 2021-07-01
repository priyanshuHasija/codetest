package org.example

import org.junit._
import Assert._

@Test
class AppTest {

    @Test
    def testOK() = assertTrue(CodeTest.calculate_perc(90,100) == 90.00)

//    @Test
//    def testKO() = assertTrue(false)

}


