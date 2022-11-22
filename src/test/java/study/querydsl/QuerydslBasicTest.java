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
    @DisplayName("JPQL 방식")
    public void startJPQL() {
        //member1을 찾아라
        Member findMember = em.createQuery("select m from Member m where m.username = :username", Member.class)
                .setParameter("username", "member1").getSingleResult();

        assertThat(findMember.getUsername()).isEqualTo("member1");
    }

    @Test
    @DisplayName("queryDSL 방식")
    public void startQueryDSL() {
        // given
        //JPQL factory는 필드레벨로 뺌
        //QMember qMember = QMember.member;
        //QMember m1 = new Qmember("m1")    -> 같은 테이블을 조인하는 경우에는 선언. 그렇지 않은 경우는 딱히 필요 없음
        //Qmember는 스태틱으로 뺌

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
    @DisplayName("검색쿼리")
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
    @DisplayName("검색쿼리")
    public void searchAndParam() {
        // when
        Member findMember = queryFactory
                .selectFrom(member)
                .where(
                        member.username.eq("member1"),
                        member.age.eq(10)
                )                                   //-> 동적쿼리 만들 때 유용
                .fetchOne();

        // then
        assertThat(findMember.getUsername()).isEqualTo("member1");
        assertThat(findMember.getAge()).isEqualTo(10);
    }

    @Test
    @DisplayName("QueryDSL 종류")
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
     * 회원 정렬 순서
     * 1. 회원 나이 내림차순
     * 2. 회원 이름 오름차순
     * 단 2에서 회원 이름이 없으면 마지막에 출력 (nulls last)
     */
    @Test
    @DisplayName("QueryDSL 으로 정렬")
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
    @DisplayName("집계함수 테스트")
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
    @DisplayName("팀의 이름과 각 팀의 평균 연령을 구하라")
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
     * 팀 A에 소속된 모든 회원을 찾아라.
     */
    @Test
    @DisplayName("QueryDSL join문 활용")
    public void join() {
        List<Member> result = queryFactory
                .selectFrom(member)
                .join(member.team, team)            //leftJoin, rightJoin 가능
                .where(team.name.eq("teamA"))
                .fetch();

        assertThat(result)
                .extracting("username")
                .containsExactly("member1", "member2");
    }

    /**
     * 세타 조인
     * 회원의 이름이 팀 이름고 같은 회원 조회
     */
    @Test
    @DisplayName("세타 조인")
    public void theta_join() {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        // when         ->cross join 발생 카티션 프로덕트
        List<Member> result = queryFactory
                .select(member)
                .from(member, team)                 //leftJoin, rightJoin 불가능
                .where(member.username.eq(team.name))
                .fetch();

        // then
        assertThat(result)
                .extracting("username")
                .containsExactly("teamA", "teamB");
    }

    /**
     * 예) 회원과 팀을 조인하면서, 팀 이름이 teamA인 팀만 조인, 회원은 모두 조회
     * JPQL : select m, t from Member m left join m.team t on t.name = 'teamA'
     */
    @Test
    @DisplayName("연관관계가 없는 테이블 조인")
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
     * 이너 조인 레프트 조인 피피티 추가
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
     * 연관관계 없는 엔티티 외부 조인    (서로 관계가 없는 필드로 외부 조인 가능)
     * 회원의 이름이 팀 이름고 같은 대상 외부 조인
     */
    @Test
    @DisplayName("세타 조인")
    public void join_on_no_relation() {
        // given
        em.persist(new Member("teamA"));
        em.persist(new Member("teamB"));

        // when         ->cross join 발생 카티션 프로덕트
        List<Tuple> result = queryFactory
                .select(member, team)
                .from(member)                 //leftJoin, rightJoin 불가능
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
    @DisplayName("패치 조인 없을 떄")
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
        assertThat(loaded).as("패치 조인 미적용").isFalse();
    }

    @Test
    @DisplayName("패치 조인 없을 떄")
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
        assertThat(loaded).as("패치 조인 미적용").isTrue();
    }

    @Test
    @DisplayName("나이가 가장 많은 회원 조회")
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
    @DisplayName("나이가 평균 이상인 회원 조회")
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
    @DisplayName("나이가 평균 이상인 회원 조회 (추천 x 효율적이지 않음)")
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
                        .when(10).then("열살")
                        .when(20).then("스무살")
                        .otherwise("기타"))
                .from(member)
                .fetch();

        // then
        for (String s : result) {
            System.out.println("s = " + s);
        }
    }

    @Test
    @DisplayName("복잡한 케이스")
    public void complexCase() {
        // when
        List<String> result = queryFactory
                .select(new CaseBuilder()
                        .when(member.age.between(0, 20)).then("0~20살")
                        .when(member.age.between(21, 30)).then("21~30살")
                        .otherwise("기타"))
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
    @DisplayName("JPQL로 DTO조회하기")
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
     * 기본생성자 호출 후 setter로 값 변경
     */
    @Test
    @DisplayName("QueryDSL로 Dto 한번에 조회하기")
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
     * 필드에 한번에 꽂아 넣음 (필드 생성자 없어도 됨)
     */
    @Test
    @DisplayName("QueryDSL로 Dto 한번에 조회하기 필드에 한번에 꽂아 넣기")
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
     * 필드 생성자 활용해서 넣기
     */
    @Test
    @DisplayName("QueryDSL로 MemebrDto 한번에 조회하기 필드 생성자 활용해서 넣기(생성자 매개변수 순서 맞아야함)")
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
     * 필드에 한번에 꽂아 넣음 (필드 생성자 없어도 됨) 필드 이름이 다를 때 해결방안
     */
    @Test
    @DisplayName("QueryDSL로 UserDto 한번에 조회하기 필드에 한번에 꽂아 넣기")
    public void findUserDtoByField() {
        // given
        // when
        List<UserDto> result = queryFactory
                .select(Projections.fields(UserDto.class, member.username.as("name"), member.age))      //UserDto의 필드는 username이 아니라 name이기때문에 as로 이름을 변한 해줘야 함
                .from(member)
                .fetch();

        // then
        for (UserDto userDto : result) {
            System.out.println("userDto = " + userDto);
        }

    }

    /**
     * 필드에 한번에 꽂아 넣음 (필드 생성자 없어도 됨) 필드 이름이 다를 때 해결 방안
     */
    @Test
    @DisplayName("QueryDSL로 UserDto 한번에 조회하기 필드에 한번에 꽂아 넣기")
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
     * 기본 생성자로 setter로 꽂아 넣기 오류를 컴파일 시점에 알지 못하고 코드가 실행 됐을 때 알 수 있음
     */
    @Test
    @DisplayName("QueryDSL로 UserDto 한번에 조회하기 기본 생성자로 setter로 꽂아넣기")
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
    @DisplayName("Dto의 생성자에 @QueryProjection을 통해 값 넣기 오류를 런타임 시점에 알 수 있음")
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
     * --------- 동적쿼리 부분 ----------
     */

    @Test
    @DisplayName("동적쿼리 null인지 아닌지 조회해서 where문에 삽입")
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

        BooleanBuilder builder = new BooleanBuilder();          //초기에 넣을 수도 있음
        if (usernameCond != null) {
            builder.and(member.username.eq(usernameCond));
        }

        if (ageCond != null) {
            builder.and(member.age.eq(ageCond));
        }

        return queryFactory
                .selectFrom(member)
                .where(builder)                                 //builder.and로도 가능
                .fetch();
    }
    
    @Test
    @DisplayName("동적쿼리 null인지 아닌지 조회해서 where문에 삽입 (다중 파라미터)")
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
//                .where(allEq(usernameCond, ageCond))                  //null 처리 필요
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
     * --------- 동적쿼리 부분 끝 ----------
     */

    @Test
    @DisplayName("벌크 연산(문제점 존재)")
    @Commit
    public void bulkUpdate() {
        // given

        //영속성은 바뀌지 않음 벌크 연산의 문제점 설렉트 쿼리는 나가는데 1차 캐시가 우선이므로 설렉트로 가져온 데이터를 버림
        //member1 = 10 > DB 비회원
        //member2 = 20 > DB 비회원
        //member3 = 30 > DB member3
        //member4 = 40 > DB member4

        // when
        long cnt = queryFactory
                .update(member)
                .set(member.username, "비회원")
                .where(member.age.lt(28))
                .execute();

        em.flush();
        em.clear();     //초기화하면 정상 작동

        List<Member> result = queryFactory
                .selectFrom(member)
                .fetch();

        // then
        for (Member member1 : result) {
            System.out.println("member1 = " + member1);
        }
    }

    @Test
    @DisplayName("모든 회원의 나이를 1 올리기")
    public void bulkAdd() {
        // given

        // when
        long cnt = queryFactory
                .update(member)
                .set(member.age, member.age.add(1))         //빼고 샆으면 -1을 함 곱하기는 멀티플라이
                .execute();
        // then
        assertThat(cnt).isEqualTo(4);
    }

    @Test
    @DisplayName("삭제 연산")
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
}