
CREATE TYPE ptt_operation AS ENUM ('push', 'dislike', 'arrow');

CREATE TABLE public.post
(
   "time" timestamp without time zone NOT NULL,
   subject character varying(256) NOT NULL,
   id character(18) NOT NULL,
   board character (12) NOT NULL,

   user_id character varying(256) NOT NULL,
   CONSTRAINT "Post.PK" PRIMARY KEY (id, board)
) ;
CREATE TABLE public.user
(
   id character varying(256) NOT NULL,
   CONSTRAINT "User.PK" PRIMARY KEY (id)
) ;

CREATE TABLE public.action
(
   user_id character varying(256) NOT NULL,
   "time" timestamp without time zone NOT NULL,
   op ptt_operation NOT NULL,
   post_id character(18) NOT NULL,
   board character (12) NOT NULL,
   content text,
   CONSTRAINT "Action.PK" PRIMARY KEY (user_id, "time", op, post_id, board)
)
;


CREATE INDEX ON public.post ("time");
CREATE INDEX ON public.post (user_id);
CREATE INDEX ON public.post (board);

CREATE INDEX ON public.action ("time");
CREATE INDEX ON public.action (post_id);
CREATE INDEX ON public.action (user_id);
CREATE INDEX ON public.action (op);
CREATE INDEX ON public.action (post_id, op);
CREATE INDEX ON public.action (user_id, op);
CREATE INDEX ON public.action ("time", post_id);
CREATE INDEX ON public.action ("time", user_id);



