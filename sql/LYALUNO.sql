--select  concurso,candidato, ano_ingresso, FORMAT(dt_ingresso , 'dd-MM-yyyy') as 'dt_matricula' from ly_aluno where candidato is not null and concurso is not null
--and ano_ingresso > 2010


select  concurso,candidato, ano_ingresso, FORMAT(dt_ingresso , 'yyyy-MM-dd') as 'dt_matricula' from ly_aluno where candidato is not null and concurso is not null
and ano_ingresso > 2015