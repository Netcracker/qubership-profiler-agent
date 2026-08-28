package com.netcracker.profiler.test.synthetic;

import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

public class Waiter {
    private static final int RUN_COUNT = 5000;
    private static final long WAITING = 1;
    private static final int ENTRYPOINT = 199;

    @RepeatedTest(10)
    @Order(1)
    public static void test() {
        for (int i = RUN_COUNT; i > 0; i--) {
            try {
                call(ENTRYPOINT);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @Order(2)
    public void sleep() throws InterruptedException {
        TimeUnit.SECONDS.sleep(1);
    }

    static int i;

    private static void $0(int next) throws Exception {
        switch (next) {
            case 0:
                waiting();
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static void waiting() throws Exception {
        i++;
//        Thread t = Thread.currentThread();
//        synchronized (t) {
//            t.wait(WAITING);
//        }
    }

    public static void call(int next) throws Exception {
        switch (next) {
            case 0:
                $0(next);
                break;
            case 1:
                $1(next);
                break;
            case 2:
                $2(next);
                break;
            case 3:
                $3(next);
                break;
            case 4:
                $4(next);
                break;
            case 5:
                $5(next);
                break;
            case 6:
                $6(next);
                break;
            case 7:
                $7(next);
                break;
            case 8:
                $8(next);
                break;
            case 9:
                $9(next);
                break;
            case 10:
                $10(next);
                break;
            case 11:
                $11(next);
                break;
            case 12:
                $12(next);
                break;
            case 13:
                $13(next);
                break;
            case 14:
                $14(next);
                break;
            case 15:
                $15(next);
                break;
            case 16:
                $16(next);
                break;
            case 17:
                $17(next);
                break;
            case 18:
                $18(next);
                break;
            case 19:
                $19(next);
                break;
            case 20:
                $20(next);
                break;
            case 21:
                $21(next);
                break;
            case 22:
                $22(next);
                break;
            case 23:
                $23(next);
                break;
            case 24:
                $24(next);
                break;
            case 25:
                $25(next);
                break;
            case 26:
                $26(next);
                break;
            case 27:
                $27(next);
                break;
            case 28:
                $28(next);
                break;
            case 29:
                $29(next);
                break;
            case 30:
                $30(next);
                break;
            case 31:
                $31(next);
                break;
            case 32:
                $32(next);
                break;
            case 33:
                $33(next);
                break;
            case 34:
                $34(next);
                break;
            case 35:
                $35(next);
                break;
            case 36:
                $36(next);
                break;
            case 37:
                $37(next);
                break;
            case 38:
                $38(next);
                break;
            case 39:
                $39(next);
                break;
            case 40:
                $40(next);
                break;
            case 41:
                $41(next);
                break;
            case 42:
                $42(next);
                break;
            case 43:
                $43(next);
                break;
            case 44:
                $44(next);
                break;
            case 45:
                $45(next);
                break;
            case 46:
                $46(next);
                break;
            case 47:
                $47(next);
                break;
            case 48:
                $48(next);
                break;
            case 49:
                $49(next);
                break;
            case 50:
                $50(next);
                break;
            case 51:
                $51(next);
                break;
            case 52:
                $52(next);
                break;
            case 53:
                $53(next);
                break;
            case 54:
                $54(next);
                break;
            case 55:
                $55(next);
                break;
            case 56:
                $56(next);
                break;
            case 57:
                $57(next);
                break;
            case 58:
                $58(next);
                break;
            case 59:
                $59(next);
                break;
            case 60:
                $60(next);
                break;
            case 61:
                $61(next);
                break;
            case 62:
                $62(next);
                break;
            case 63:
                $63(next);
                break;
            case 64:
                $64(next);
                break;
            case 65:
                $65(next);
                break;
            case 66:
                $66(next);
                break;
            case 67:
                $67(next);
                break;
            case 68:
                $68(next);
                break;
            case 69:
                $69(next);
                break;
            case 70:
                $70(next);
                break;
            case 71:
                $71(next);
                break;
            case 72:
                $72(next);
                break;
            case 73:
                $73(next);
                break;
            case 74:
                $74(next);
                break;
            case 75:
                $75(next);
                break;
            case 76:
                $76(next);
                break;
            case 77:
                $77(next);
                break;
            case 78:
                $78(next);
                break;
            case 79:
                $79(next);
                break;
            case 80:
                $80(next);
                break;
            case 81:
                $81(next);
                break;
            case 82:
                $82(next);
                break;
            case 83:
                $83(next);
                break;
            case 84:
                $84(next);
                break;
            case 85:
                $85(next);
                break;
            case 86:
                $86(next);
                break;
            case 87:
                $87(next);
                break;
            case 88:
                $88(next);
                break;
            case 89:
                $89(next);
                break;
            case 90:
                $90(next);
                break;
            case 91:
                $91(next);
                break;
            case 92:
                $92(next);
                break;
            case 93:
                $93(next);
                break;
            case 94:
                $94(next);
                break;
            case 95:
                $95(next);
                break;
            case 96:
                $96(next);
                break;
            case 97:
                $97(next);
                break;
            case 98:
                $98(next);
                break;
            case 99:
                $99(next);
                break;
            case 100:
                $100(next);
                break;
            case 101:
                $101(next);
                break;
            case 102:
                $102(next);
                break;
            case 103:
                $103(next);
                break;
            case 104:
                $104(next);
                break;
            case 105:
                $105(next);
                break;
            case 106:
                $106(next);
                break;
            case 107:
                $107(next);
                break;
            case 108:
                $108(next);
                break;
            case 109:
                $109(next);
                break;
            case 110:
                $110(next);
                break;
            case 111:
                $111(next);
                break;
            case 112:
                $112(next);
                break;
            case 113:
                $113(next);
                break;
            case 114:
                $114(next);
                break;
            case 115:
                $115(next);
                break;
            case 116:
                $116(next);
                break;
            case 117:
                $117(next);
                break;
            case 118:
                $118(next);
                break;
            case 119:
                $119(next);
                break;
            case 120:
                $120(next);
                break;
            case 121:
                $121(next);
                break;
            case 122:
                $122(next);
                break;
            case 123:
                $123(next);
                break;
            case 124:
                $124(next);
                break;
            case 125:
                $125(next);
                break;
            case 126:
                $126(next);
                break;
            case 127:
                $127(next);
                break;
            case 128:
                $128(next);
                break;
            case 129:
                $129(next);
                break;
            case 130:
                $130(next);
                break;
            case 131:
                $131(next);
                break;
            case 132:
                $132(next);
                break;
            case 133:
                $133(next);
                break;
            case 134:
                $134(next);
                break;
            case 135:
                $135(next);
                break;
            case 136:
                $136(next);
                break;
            case 137:
                $137(next);
                break;
            case 138:
                $138(next);
                break;
            case 139:
                $139(next);
                break;
            case 140:
                $140(next);
                break;
            case 141:
                $141(next);
                break;
            case 142:
                $142(next);
                break;
            case 143:
                $143(next);
                break;
            case 144:
                $144(next);
                break;
            case 145:
                $145(next);
                break;
            case 146:
                $146(next);
                break;
            case 147:
                $147(next);
                break;
            case 148:
                $148(next);
                break;
            case 149:
                $149(next);
                break;
            case 150:
                $150(next);
                break;
            case 151:
                $151(next);
                break;
            case 152:
                $152(next);
                break;
            case 153:
                $153(next);
                break;
            case 154:
                $154(next);
                break;
            case 155:
                $155(next);
                break;
            case 156:
                $156(next);
                break;
            case 157:
                $157(next);
                break;
            case 158:
                $158(next);
                break;
            case 159:
                $159(next);
                break;
            case 160:
                $160(next);
                break;
            case 161:
                $161(next);
                break;
            case 162:
                $162(next);
                break;
            case 163:
                $163(next);
                break;
            case 164:
                $164(next);
                break;
            case 165:
                $165(next);
                break;
            case 166:
                $166(next);
                break;
            case 167:
                $167(next);
                break;
            case 168:
                $168(next);
                break;
            case 169:
                $169(next);
                break;
            case 170:
                $170(next);
                break;
            case 171:
                $171(next);
                break;
            case 172:
                $172(next);
                break;
            case 173:
                $173(next);
                break;
            case 174:
                $174(next);
                break;
            case 175:
                $175(next);
                break;
            case 176:
                $176(next);
                break;
            case 177:
                $177(next);
                break;
            case 178:
                $178(next);
                break;
            case 179:
                $179(next);
                break;
            case 180:
                $180(next);
                break;
            case 181:
                $181(next);
                break;
            case 182:
                $182(next);
                break;
            case 183:
                $183(next);
                break;
            case 184:
                $184(next);
                break;
            case 185:
                $185(next);
                break;
            case 186:
                $186(next);
                break;
            case 187:
                $187(next);
                break;
            case 188:
                $188(next);
                break;
            case 189:
                $189(next);
                break;
            case 190:
                $190(next);
                break;
            case 191:
                $191(next);
                break;
            case 192:
                $192(next);
                break;
            case 193:
                $193(next);
                break;
            case 194:
                $194(next);
                break;
            case 195:
                $195(next);
                break;
            case 196:
                $196(next);
                break;
            case 197:
                $197(next);
                break;
            case 198:
                $198(next);
                break;
            case 199:
                $199(next);
                break;
            case 200:
                $199(next);
                break;
        }
    }

    private static void $1(int next) throws Exception {
        call(--next);
    }

    private static void $2(int next) throws Exception {
        call(--next);
    }

    private static void $3(int next) throws Exception {
        call(--next);
    }

    private static void $4(int next) throws Exception {
        call(--next);
    }

    private static void $5(int next) throws Exception {
        call(--next);
    }

    private static void $6(int next) throws Exception {
        call(--next);
    }

    private static void $7(int next) throws Exception {
        call(--next);
    }

    private static void $8(int next) throws Exception {
        call(--next);
    }

    private static void $9(int next) throws Exception {
        call(--next);
    }

    private static void $10(int next) throws Exception {
        call(--next);
    }

    private static void $11(int next) throws Exception {
        call(--next);
    }

    private static void $12(int next) throws Exception {
        call(--next);
    }

    private static void $13(int next) throws Exception {
        call(--next);
    }

    private static void $14(int next) throws Exception {
        call(--next);
    }

    private static void $15(int next) throws Exception {
        call(--next);
    }

    private static void $16(int next) throws Exception {
        call(--next);
    }

    private static void $17(int next) throws Exception {
        call(--next);
    }

    private static void $18(int next) throws Exception {
        call(--next);
    }

    private static void $19(int next) throws Exception {
        call(--next);
    }

    private static void $20(int next) throws Exception {
        call(--next);
    }

    private static void $21(int next) throws Exception {
        call(--next);
    }

    private static void $22(int next) throws Exception {
        call(--next);
    }

    private static void $23(int next) throws Exception {
        call(--next);
    }

    private static void $24(int next) throws Exception {
        call(--next);
    }

    private static void $25(int next) throws Exception {
        call(--next);
    }

    private static void $26(int next) throws Exception {
        call(--next);
    }

    private static void $27(int next) throws Exception {
        call(--next);
    }

    private static void $28(int next) throws Exception {
        call(--next);
    }

    private static void $29(int next) throws Exception {
        call(--next);
    }

    private static void $30(int next) throws Exception {
        call(--next);
    }

    private static void $31(int next) throws Exception {
        call(--next);
    }

    private static void $32(int next) throws Exception {
        call(--next);
    }

    private static void $33(int next) throws Exception {
        call(--next);
    }

    private static void $34(int next) throws Exception {
        call(--next);
    }

    private static void $35(int next) throws Exception {
        call(--next);
    }

    private static void $36(int next) throws Exception {
        call(--next);
    }

    private static void $37(int next) throws Exception {
        call(--next);
    }

    private static void $38(int next) throws Exception {
        call(--next);
    }

    private static void $39(int next) throws Exception {
        call(--next);
    }

    private static void $40(int next) throws Exception {
        call(--next);
    }

    private static void $41(int next) throws Exception {
        call(--next);
    }

    private static void $42(int next) throws Exception {
        call(--next);
    }

    private static void $43(int next) throws Exception {
        call(--next);
    }

    private static void $44(int next) throws Exception {
        call(--next);
    }

    private static void $45(int next) throws Exception {
        call(--next);
    }

    private static void $46(int next) throws Exception {
        call(--next);
    }

    private static void $47(int next) throws Exception {
        call(--next);
    }

    private static void $48(int next) throws Exception {
        call(--next);
    }

    private static void $49(int next) throws Exception {
        call(--next);
    }

    private static void $50(int next) throws Exception {
        call(--next);
    }

    private static void $51(int next) throws Exception {
        call(--next);
    }

    private static void $52(int next) throws Exception {
        call(--next);
    }

    private static void $53(int next) throws Exception {
        call(--next);
    }

    private static void $54(int next) throws Exception {
        call(--next);
    }

    private static void $55(int next) throws Exception {
        call(--next);
    }

    private static void $56(int next) throws Exception {
        call(--next);
    }

    private static void $57(int next) throws Exception {
        call(--next);
    }

    private static void $58(int next) throws Exception {
        call(--next);
    }

    private static void $59(int next) throws Exception {
        call(--next);
    }

    private static void $60(int next) throws Exception {
        call(--next);
    }

    private static void $61(int next) throws Exception {
        call(--next);
    }

    private static void $62(int next) throws Exception {
        call(--next);
    }

    private static void $63(int next) throws Exception {
        call(--next);
    }

    private static void $64(int next) throws Exception {
        call(--next);
    }

    private static void $65(int next) throws Exception {
        call(--next);
    }

    private static void $66(int next) throws Exception {
        call(--next);
    }

    private static void $67(int next) throws Exception {
        call(--next);
    }

    private static void $68(int next) throws Exception {
        call(--next);
    }

    private static void $69(int next) throws Exception {
        call(--next);
    }

    private static void $70(int next) throws Exception {
        call(--next);
    }

    private static void $71(int next) throws Exception {
        call(--next);
    }

    private static void $72(int next) throws Exception {
        call(--next);
    }

    private static void $73(int next) throws Exception {
        call(--next);
    }

    private static void $74(int next) throws Exception {
        call(--next);
    }

    private static void $75(int next) throws Exception {
        call(--next);
    }

    private static void $76(int next) throws Exception {
        call(--next);
    }

    private static void $77(int next) throws Exception {
        call(--next);
    }

    private static void $78(int next) throws Exception {
        call(--next);
    }

    private static void $79(int next) throws Exception {
        call(--next);
    }

    private static void $80(int next) throws Exception {
        call(--next);
    }

    private static void $81(int next) throws Exception {
        call(--next);
    }

    private static void $82(int next) throws Exception {
        call(--next);
    }

    private static void $83(int next) throws Exception {
        call(--next);
    }

    private static void $84(int next) throws Exception {
        call(--next);
    }

    private static void $85(int next) throws Exception {
        call(--next);
    }

    private static void $86(int next) throws Exception {
        call(--next);
    }

    private static void $87(int next) throws Exception {
        call(--next);
    }

    private static void $88(int next) throws Exception {
        call(--next);
    }

    private static void $89(int next) throws Exception {
        call(--next);
    }

    private static void $90(int next) throws Exception {
        call(--next);
    }

    private static void $91(int next) throws Exception {
        call(--next);
    }

    private static void $92(int next) throws Exception {
        call(--next);
    }

    private static void $93(int next) throws Exception {
        call(--next);
    }

    private static void $94(int next) throws Exception {
        call(--next);
    }

    private static void $95(int next) throws Exception {
        call(--next);
    }

    private static void $96(int next) throws Exception {
        call(--next);
    }

    private static void $97(int next) throws Exception {
        call(--next);
    }

    private static void $98(int next) throws Exception {
        call(--next);
    }

    private static void $99(int next) throws Exception {
        call(--next);
    }

    private static void $100(int next) throws Exception {
        call(--next);
    }

    private static void $101(int next) throws Exception {
        call(--next);
    }

    private static void $102(int next) throws Exception {
        call(--next);
    }

    private static void $103(int next) throws Exception {
        call(--next);
    }

    private static void $104(int next) throws Exception {
        call(--next);
    }

    private static void $105(int next) throws Exception {
        call(--next);
    }

    private static void $106(int next) throws Exception {
        call(--next);
    }

    private static void $107(int next) throws Exception {
        call(--next);
    }

    private static void $108(int next) throws Exception {
        call(--next);
    }

    private static void $109(int next) throws Exception {
        call(--next);
    }

    private static void $110(int next) throws Exception {
        call(--next);
    }

    private static void $111(int next) throws Exception {
        call(--next);
    }

    private static void $112(int next) throws Exception {
        call(--next);
    }

    private static void $113(int next) throws Exception {
        call(--next);
    }

    private static void $114(int next) throws Exception {
        call(--next);
    }

    private static void $115(int next) throws Exception {
        call(--next);
    }

    private static void $116(int next) throws Exception {
        call(--next);
    }

    private static void $117(int next) throws Exception {
        call(--next);
    }

    private static void $118(int next) throws Exception {
        call(--next);
    }

    private static void $119(int next) throws Exception {
        call(--next);
    }

    private static void $120(int next) throws Exception {
        call(--next);
    }

    private static void $121(int next) throws Exception {
        call(--next);
    }

    private static void $122(int next) throws Exception {
        call(--next);
    }

    private static void $123(int next) throws Exception {
        call(--next);
    }

    private static void $124(int next) throws Exception {
        call(--next);
    }

    private static void $125(int next) throws Exception {
        call(--next);
    }

    private static void $126(int next) throws Exception {
        call(--next);
    }

    private static void $127(int next) throws Exception {
        call(--next);
    }

    private static void $128(int next) throws Exception {
        call(--next);
    }

    private static void $129(int next) throws Exception {
        call(--next);
    }

    private static void $130(int next) throws Exception {
        call(--next);
    }

    private static void $131(int next) throws Exception {
        call(--next);
    }

    private static void $132(int next) throws Exception {
        call(--next);
    }

    private static void $133(int next) throws Exception {
        call(--next);
    }

    private static void $134(int next) throws Exception {
        call(--next);
    }

    private static void $135(int next) throws Exception {
        call(--next);
    }

    private static void $136(int next) throws Exception {
        call(--next);
    }

    private static void $137(int next) throws Exception {
        call(--next);
    }

    private static void $138(int next) throws Exception {
        call(--next);
    }

    private static void $139(int next) throws Exception {
        call(--next);
    }

    private static void $140(int next) throws Exception {
        call(--next);
    }

    private static void $141(int next) throws Exception {
        call(--next);
    }

    private static void $142(int next) throws Exception {
        call(--next);
    }

    private static void $143(int next) throws Exception {
        call(--next);
    }

    private static void $144(int next) throws Exception {
        call(--next);
    }

    private static void $145(int next) throws Exception {
        call(--next);
    }

    private static void $146(int next) throws Exception {
        call(--next);
    }

    private static void $147(int next) throws Exception {
        call(--next);
    }

    private static void $148(int next) throws Exception {
        call(--next);
    }

    private static void $149(int next) throws Exception {
        call(--next);
    }

    private static void $150(int next) throws Exception {
        call(--next);
    }

    private static void $151(int next) throws Exception {
        call(--next);
    }

    private static void $152(int next) throws Exception {
        call(--next);
    }

    private static void $153(int next) throws Exception {
        call(--next);
    }

    private static void $154(int next) throws Exception {
        call(--next);
    }

    private static void $155(int next) throws Exception {
        call(--next);
    }

    private static void $156(int next) throws Exception {
        call(--next);
    }

    private static void $157(int next) throws Exception {
        call(--next);
    }

    private static void $158(int next) throws Exception {
        call(--next);
    }

    private static void $159(int next) throws Exception {
        call(--next);
    }

    private static void $160(int next) throws Exception {
        call(--next);
    }

    private static void $161(int next) throws Exception {
        call(--next);
    }

    private static void $162(int next) throws Exception {
        call(--next);
    }

    private static void $163(int next) throws Exception {
        call(--next);
    }

    private static void $164(int next) throws Exception {
        call(--next);
    }

    private static void $165(int next) throws Exception {
        call(--next);
    }

    private static void $166(int next) throws Exception {
        call(--next);
    }

    private static void $167(int next) throws Exception {
        call(--next);
    }

    private static void $168(int next) throws Exception {
        call(--next);
    }

    private static void $169(int next) throws Exception {
        call(--next);
    }

    private static void $170(int next) throws Exception {
        call(--next);
    }

    private static void $171(int next) throws Exception {
        call(--next);
    }

    private static void $172(int next) throws Exception {
        call(--next);
    }

    private static void $173(int next) throws Exception {
        call(--next);
    }

    private static void $174(int next) throws Exception {
        call(--next);
    }

    private static void $175(int next) throws Exception {
        call(--next);
    }

    private static void $176(int next) throws Exception {
        call(--next);
    }

    private static void $177(int next) throws Exception {
        call(--next);
    }

    private static void $178(int next) throws Exception {
        call(--next);
    }

    private static void $179(int next) throws Exception {
        call(--next);
    }

    private static void $180(int next) throws Exception {
        call(--next);
    }

    private static void $181(int next) throws Exception {
        call(--next);
    }

    private static void $182(int next) throws Exception {
        call(--next);
    }

    private static void $183(int next) throws Exception {
        call(--next);
    }

    private static void $184(int next) throws Exception {
        call(--next);
    }

    private static void $185(int next) throws Exception {
        call(--next);
    }

    private static void $186(int next) throws Exception {
        call(--next);
    }

    private static void $187(int next) throws Exception {
        call(--next);
    }

    private static void $188(int next) throws Exception {
        call(--next);
    }

    private static void $189(int next) throws Exception {
        call(--next);
    }

    private static void $190(int next) throws Exception {
        call(--next);
    }

    private static void $191(int next) throws Exception {
        call(--next);
    }

    private static void $192(int next) throws Exception {
        call(--next);
    }

    private static void $193(int next) throws Exception {
        call(--next);
    }

    private static void $194(int next) throws Exception {
        call(--next);
    }

    private static void $195(int next) throws Exception {
        call(--next);
    }

    private static void $196(int next) throws Exception {
        call(--next);
    }

    private static void $197(int next) throws Exception {
        call(--next);
    }

    private static void $198(int next) throws Exception {
        call(--next);
    }

    private static void $199(int next) throws Exception {
        call(--next);
    }

    private static void $200(int next) throws Exception {
        call(--next);
    }

}
