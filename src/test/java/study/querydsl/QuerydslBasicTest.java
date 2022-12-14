package study.querydsl;

import com.querydsl.core.BooleanBuilder;
import com.querydsl.core.QueryResults;
import com.querydsl.core.Tuple;
import com.querydsl.core.types.ExpressionUtils;
import com.querydsl.core.types.Predicate;
import com.querydsl.core.types.Projections;
import com.querydsl.core.types.dsl.BooleanExpression;
import com.querydsl.core.types.dsl.CaseBuilder;
import com.querydsl.core.types.dsl.Expressions;
import com.querydsl.jpa.JPAExpressions;
import com.querydsl.jpa.impl.JPAQueryFactory;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.Commit;
import org.springframework.test.annotation.Rollback;
import org.springframework.transaction.annotation.Transactional;
import study.querydsl.dto.MemberDto;
import study.querydsl.dto.QMemberDto;
import study.querydsl.dto.UserDto;
import study.querydsl.entity.Member;
import study.querydsl.entity.QMember;
import study.querydsl.entity.QTeam;
import study.querydsl.entity.Team;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.PersistenceUnit;
import java.util.List;

import static org.assertj.core.api.Assertions.*;
import static study.querydsl.entity.QMember.*;
import static study.querydsl.entity.QTeam.*;

@SpringBootTest
@Transactional
public class QuerydslBasicTest {

    @Autowired
    EntityManager em;
    JPAQueryFactory queryFactory;


    @BeforeEach
    public void before() {
        queryFactory = new JPAQueryFactory(em);
        Team teamA = new Team("teamA");
        Team teamB = new Team("teamB");
        em.persist(teamB);
        em.persist(teamA);

        Member member1 = new Member("member1", 10, teamA);
        Member member2 = new Member("member2", 20, teamA);
        Member member3 = new Member("member3", 30, teamB);
        Member member4 = new Member("member4", 40, teamB);
        em.persist(member1);
        em.persist(member2);
        em.persist(member3);
        em.persist(member4);
    }

    @Test
    @DisplayName("JPQL ??????")
    public void startJPQL() {
        //member1??? ?????????
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1").getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    @DisplayName("queryDSL ??????")
    public void startQueryDSL() {
        // given
        //JPQL factory??? ??????????????? ???
        //QMember qMember = QMember.member;
        //QMember m1 = new Qmember("m1")    -> ?????? ???????????? ???????????? ???????????? ??????. ????????? ?????? ????????? ?????? ?????? ??????
        //Qmember??? ??????????????? ???

        // when
        Member findMember = queryFactory
                .select(member)
                .from(member)
                .where(member.username.eq("member1"))
                .fetchOne();

        // then
        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    @DisplayName("????????????")
    public void search() {
        // when
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member1")
                        .and(member.age.eq(10)))
                .fetchOne();

        // then
        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }

    @Test
    @DisplayName("????????????")
    public void searchAndParam() {
        // when
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )                                   //-> ???????????? ?????? ??? ??????
                .fetchOne();

        // then
        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }

    @Test
    @DisplayName("QueryDSL ??????")
    public void resultFetch() {
//        List<Member> fetch = queryFactory.selectFrom(member).fetch();
//
//        Member fetchOne = queryFactory.selectFrom(member).where(member.username.eq("member1")).fetchOne();
//
//        Member fetchFirst = queryFactory.selectFrom(member).fetchFirst();

        QueryResults<Member> results = queryFactory.selectFrom(member).fetchResults();

        results.getTotal();
        List<Member> content = results.getResults();
    }

    /**
     * ?????? ?????? ??????
     * 1. ?????? ?????? ????????????
     * 2. ?????? ?????? ????????????
     * ??? 2?????? ?????? ????????? ????????? ???????????? ?????? (nulls last)
     */
    @Test
    @DisplayName("QueryDSL ?????? ??????")
    public void sort() {
        // given
        em.persist(new Member(null, 100));
        em.persist(new Member("member5", 100));
        em.persist(new Member("member6", 100));

        // when
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(100))
                .orderBy(member.age.desc(), member.username.asc().nullsLast())
                .fetch();

        // then
        Member member5 = result.get(0);
        Member member6 = result.get(1);
        Member memberNull = result.get(2);

        assertThat(member5.getUsername()).isEqualTo("member5");
        assertThat(member6.getUsername()).isEqualTo("member6");
        assertThat(memberNull.getUsername()).isNull();

    }

    @Test
    @DisplayName("QueryDSL paging")
    public void paging() {
        QueryResults<Member> result = queryFactory
                .selectFrom(member)
                .orderBy(member.username.desc())
                .offset(1)
                .limit(2)
                .fetchResults();

        assertThat(result.getTotal()).isEqualTo(4);
        assertThat(result.getLimit()).isEqualTo(2);
        assertThat(result.getOffset()).isEqualTo(1);
        assertThat(result.getResults().size()).isEqualTo(2);
    }

    @Test
    @DisplayName("???????????? ?????????")
    public void aggregation() {
        List<Tuple> result = queryFactory.select(
                        member.count(),
                        member.age.sum(),
                        member.age.max(),
                        member.age.avg(),
                        member.age.min())
                .from(member)
                .fetch();

        Tuple tuple = result.get(0);
        assertThat(tuple.get(member.count())).isEqualTo(4);
        assertThat(tuple.get(member.age.sum())).isEqualTo(100);
        assertThat(tuple.get(member.age.avg())).isEqualTo(25);
        assertThat(tuple.get(member.age.max())).isEqualTo(40);
        assertThat(tuple.get(member.age.min())).isEqualTo(10);

    }

    @Test
    @DisplayName("?????? ????????? ??? ?????? ?????? ????????? ?????????")
    public void group() {
        List<Tuple> result = queryFactory
                .select(team.name, member.age.avg())
                .from(member)
                .join(member.team, team)
                .groupBy(team.name)
                .fetch();

        Tuple teamA = result.get(0);
        Tuple teamB = result.get(1);

        assertThat(teamA.get(team.name)).isEqualTo("teamA");
        assertThat(teamA.get(member.age.avg())).isEqualTo(15);  //(10+20) / 2

        assertThat(teamB.get(team.name)).isEqualTo("teamB");
        assertThat(teamB.get(member.age.avg())).isEqualTo(35); //(30+40) / 2
    }

    /**
     * ??? A??? ????????? ?????? ????????? ?????????.
     */
    @Test
    @DisplayName("QueryDSL join??? ??????")
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)           //leftJoin, rightJoin ??????
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * ?????? ??????
     * ????????? ????????? ??? ????????? ?????? ?????? ??????
     */
    @Test
    @DisplayName("?????? ??????")
    public void theta_join() {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        // when         ->cross join ?????? ????????? ????????????
        List<Member> result = queryFactory
                .select(member)
                .from(member, team)                 //leftJoin, rightJoin ?????????
                .where(member.username.eq(team.name))
                .fetch();

        // then
        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * ???) ????????? ?????? ???????????????, ??? ????????? teamA??? ?????? ??????, ????????? ?????? ??????
     * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    @DisplayName("??????????????? ?????? ????????? ??????")
    public void join_on_filtering() {
        List<Member> result = queryFactory
                .select(member)
                .from(member, team)
                .join(member.team, team)
                .where(team.name.eq("teamA"))
                .fetch();

        for (Member member1 : result) {
            member1.getTeam().getName();
        }
    }

    /**
     * ?????? ?????? ????????? ?????? ????????? ??????
     */
    @Test
    @DisplayName("")
    public void join_on_filtering2() {
        // given
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)
//                .leftJoin(member.team, team)
                .join(member.team, team)
//                .on(team.name.eq("teamA"))
                .where(team.name.eq("teamA"))
                .fetch();

        // when
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
        // then
    }

    /**
     * ???????????? ?????? ????????? ?????? ??????    (?????? ????????? ?????? ????????? ?????? ?????? ??????)
     * ????????? ????????? ??? ????????? ?????? ?????? ?????? ??????
     */
    @Test
    @DisplayName("?????? ??????")
    public void join_on_no_relation() {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        // when         ->cross join ?????? ????????? ????????????
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)                 //leftJoin, rightJoin ?????????
                .leftJoin(team)
                .on(member.username.eq(team.name))
                .fetch();

        // then
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @PersistenceUnit
    EntityManagerFactory emf;

    @Test
    @DisplayName("?????? ?????? ?????? ???")
    public void fetchJoinNo() {
        // given
        em.flush();
        em.clear();

        // when
        Member findMember = queryFactory
                .selectFrom(member)
                .where(member.username.eq("member2"))
                .fetchOne();

        // then
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("?????? ?????? ?????????").isFalse();
    }

    @Test
    @DisplayName("?????? ?????? ?????? ???")
    public void fetchJoinUse() {
        // given
        em.flush();
        em.clear();

        // when
        Member findMember = queryFactory
                .selectFrom(member)
                .join(member.team, team).fetchJoin()
                .where(member.username.eq("member2"))
                .fetchOne();

        // then
        boolean loaded = emf.getPersistenceUnitUtil().isLoaded(findMember.getTeam());
        assertThat(loaded).as("?????? ?????? ?????????").isTrue();
    }

    @Test
    @DisplayName("????????? ?????? ?????? ?????? ??????")
    public void subQuery() {
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.eq(
                        JPAExpressions
                                .select(memberSub.age.max())
                                .from(memberSub)
                ))
                .fetch();

        // then
        assertThat(result).extracting("age")
                .containsExactly(40);
    }

    @Test
    @DisplayName("????????? ?????? ????????? ?????? ??????")
    public void subQueryGoe() {
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.goe(
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub)
                ))
                .fetch();

        // then
        assertThat(result).extracting("age")
                .containsExactly(30,40);
    }

    @Test
    @DisplayName("????????? ?????? ????????? ?????? ?????? (?????? x ??????????????? ??????)")
    public void subQueryIn() {
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Member> result = queryFactory
                .selectFrom(member)
                .where(member.age.in(
                        JPAExpressions
                                .select(memberSub.age)
                                .from(memberSub)
                                .where(memberSub.age.gt(10))
                ))
                .fetch();

        // then
        assertThat(result).extracting("age")
                .containsExactly(20, 30,40);
    }

    @Test
    @DisplayName("")
    public void selectSubQuery() {
        // given
        QMember memberSub = new QMember("memberSub");

        // when
        List<Tuple> result = queryFactory
                .select(member.username,
                        JPAExpressions
                                .select(memberSub.age.avg())
                                .from(memberSub))
                .from(member)
                .fetch();

        // then
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    @DisplayName("")
    public void basicCase() {
        // given

        // when
        List<String> result = queryFactory
                .select(member.age
                        .when(10).then("??????")
                        .when(20).then("?????????")
                        .otherwise("??????"))
                .from(member)
                .fetch();

        // then
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    @DisplayName("????????? ?????????")
    public void complexCase() {
        // when
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20???")
                        .when(member.age.between(21, 30)).then("21~30???")
                        .otherwise("??????"))
                .from(member)
                .fetch();

        // then
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    @DisplayName("")
    public void constant() {
        // when
        List<Tuple> result = queryFactory
                .select(member.username, Expressions.constant("A"))
                .from(member)
                .fetch();

        // then
        for (Tuple tuple : result) {
            System.out.println("tuple = " + tuple);
        }
    }

    @Test
    @DisplayName("")
    public void concat() {
        // when
        List<String> result = queryFactory
                .select(member.username.concat("_").concat(member.age.stringValue()))
                .from(member)
                .where(member.username.eq("member1"))
                .fetch();

        // then
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    @DisplayName("")
    public void simpleProjection() {
        // when
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
                .fetch();

        // then
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    @DisplayName("")
    public void tupleProjection() {
        // given
        List<Tuple> result = queryFactory
                .select(member.username,
                        member.age)
                .from(member)
                .fetch();
        // when

        // then
        for (Tuple tuple : result) {
            String username = tuple.get(member.username);
            Integer age = tuple.get(member.age);
            System.out.println("username = " + username);
            System.out.println("age = " + age);
        }
    }

    @Test
    @DisplayName("JPQL??? DTO????????????")
    public void findDtoByJPQL() {
        // given
        
        // when
        List<MemberDto> result = em.createQuery("select new study.querydsl.dto.MemberDto(m.username, m.age) from Member m", MemberDto.class)
                .getResultList();

        // then
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * ??????????????? ?????? ??? setter??? ??? ??????
     */
    @Test
    @DisplayName("QueryDSL??? Dto ????????? ????????????")
    public void findDtoBySetter() {
        // given
        
        // when
        List<MemberDto> result = queryFactory
                .select(Projections.bean(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        // then
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * ????????? ????????? ?????? ?????? (?????? ????????? ????????? ???)
     */
    @Test
    @DisplayName("QueryDSL??? Dto ????????? ???????????? ????????? ????????? ?????? ??????")
    public void findDtoByField() {
        // given

        // when
        List<MemberDto> result = queryFactory
                .select(Projections.fields(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        // then
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * ?????? ????????? ???????????? ??????
     */
    @Test
    @DisplayName("QueryDSL??? MemebrDto ????????? ???????????? ?????? ????????? ???????????? ??????(????????? ???????????? ?????? ????????????)")
    public void findDtoByConstructor() {
        // given

        // when
        List<MemberDto> result = queryFactory
                .select(Projections.constructor(MemberDto.class, member.username, member.age))
                .from(member)
                .fetch();

        // then
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * ????????? ????????? ?????? ?????? (?????? ????????? ????????? ???) ?????? ????????? ?????? ??? ????????????
     */
    @Test
    @DisplayName("QueryDSL??? UserDto ????????? ???????????? ????????? ????????? ?????? ??????")
    public void findUserDtoByField() {
        // given
        // when
        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class, member.username.as("name"), member.age))      //UserDto??? ????????? username??? ????????? name??????????????? as??? ????????? ?????? ????????? ???
                .from(member)
                .fetch();

        // then
        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }

    }

    /**
     * ????????? ????????? ?????? ?????? (?????? ????????? ????????? ???) ?????? ????????? ?????? ??? ?????? ??????
     */
    @Test
    @DisplayName("QueryDSL??? UserDto ????????? ???????????? ????????? ????????? ?????? ??????")
    public void findUserDtoByFieldSubQuery() {
        // given
        QMember memberSub = new QMember("memberSub");
        // when
        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class, member.username.as("name"),
                        ExpressionUtils.as(JPAExpressions
                                .select(memberSub.age.max()).from(memberSub), "age")))
                .from(member)
                .fetch();

        // then
        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    /**
     * ?????? ???????????? setter??? ?????? ?????? ????????? ????????? ????????? ?????? ????????? ????????? ?????? ?????? ??? ??? ??? ??????
     */
    @Test
    @DisplayName("QueryDSL??? UserDto ????????? ???????????? ?????? ???????????? setter??? ????????????")
    public void findUserDtoByConstructor() {
        // given

        // when
        List<UserDto> result = queryFactory
                .select(Projections.constructor(UserDto.class, member.username, member.age))
                .from(member)
                .fetch();

        // then
        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }
    }

    @Test
    @DisplayName("Dto??? ???????????? @QueryProjection??? ?????? ??? ?????? ????????? ????????? ????????? ??? ??? ??????")
    public void findDtoByQueryProjection() {
        // given
        // when
        List<MemberDto> result = queryFactory
                .select(new QMemberDto(member.username, member.age))
                .from(member)
                .fetch();

        // then
        for (MemberDto memberDto : result) {
            System.out.println("memberDto = " + memberDto);
        }
    }

    /**
     * --------- ???????????? ?????? ----------
     */

    @Test
    @DisplayName("???????????? null?????? ????????? ???????????? where?????? ??????")
    public void dynamicQuery_BooleanBuilder() {
        // given
        String usernameParam = "member1";
        Integer ageParam = null;

        // when
        List<Member> result = searchMember1(usernameParam, ageParam);

        // then
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember1(String usernameCond, Integer ageCond) {

        BooleanBuilder builder = new BooleanBuilder(/*member.username.eq(usernameCond)*/);          //????????? ?????? ?????? ??????
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)                                 //builder.and?????? ??????
                .fetch();
    }
    
    @Test
    @DisplayName("???????????? null?????? ????????? ???????????? where?????? ?????? (?????? ????????????)")
    public void dynamicQuery_whereParam() {
        // given
        String usernameParam = "member1";
        Integer ageParam = null;

        // when
        List<Member> result = searchMember2(usernameParam, ageParam);

        // then
        assertThat(result.size()).isEqualTo(1);
    }

    private List<Member> searchMember2(String usernameCond, Integer ageCond) {

        return queryFactory
                .selectFrom(member)
                .where(usernameEq(usernameCond), ageEq(ageCond))
//                .where(allEq(usernameCond, ageCond))                  //????????? ??????
                .fetch();
    }

    private BooleanExpression usernameEq(String usernameCond) {
        return usernameCond != null ? member.username.eq(usernameCond) : null;

    }

    private BooleanExpression ageEq(Integer ageCond) {
        return ageCond != null ? member.age.eq(ageCond) : null;
    }

    private BooleanExpression allEq(String usernameCond, Integer ageCond) {
        return usernameEq(usernameCond).and(ageEq(ageCond));
    }

    /**
     * --------- ???????????? ?????? ??? ----------
     */

    @Test
    @DisplayName("?????? ??????(????????? ??????)")
    @Commit
    public void bulkUpdate() {
        // given

        //???????????? ????????? ?????? ?????? ????????? ????????? ????????? ????????? ???????????? 1??? ????????? ??????????????? ???????????? ????????? ???????????? ??????
        //member1 = 10 > DB ?????????
        //member2 = 20 > DB ?????????
        //member3 = 30 > DB member3
        //member4 = 40 > DB member4

        // when
        long cnt = queryFactory
                .update(member)
                .set(member.username, "?????????")
                .where(member.age.lt(28))
                .execute();

        em.flush();
        em.clear();     //??????????????? ?????? ??????

        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        // then
        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    @DisplayName("?????? ????????? ????????? 1 ?????????")
    public void bulkAdd() {
        // given

        // when
        long cnt = queryFactory
                .update(member)
                .set(member.age, member.age.add(1))         //?????? ????????? -1??? ??? ???????????? ???????????????
                .execute();
        // then
        assertThat(cnt).isEqualTo(4);
    }

    @Test
    @DisplayName("?????? ??????")
    public void bulkDelete() {
        // given

        // when
        long cnt = queryFactory
                .delete(member)
                .where(member.age.gt(18))
                .execute();
        // then

        assertThat(cnt).isEqualTo(3);

        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    @DisplayName("")
    public void sqlFuntion() {
        // given

        // when
        List<String> result = queryFactory
                .select(Expressions.stringTemplate(
                        "function('replace', {0}, {1}, {2})",
                        member.username, "member", "M"
                ))
                .from(member)
                .fetch();

        // then
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    @DisplayName("")
    public void sqlFunction2() {
        // when
        List<String> result = queryFactory
                .select(member.username)
                .from(member)
//                .where(member.username.eq(
//                        Expressions.stringTemplate("function('lower', {0})", member.username)))
                .where(member.username.eq(member.username.lower()))
                .fetch();

        // then
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }
}